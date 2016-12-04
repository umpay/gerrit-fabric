package peer

import (
	"fmt"
        "time"
	"sync"
    "io"
	"github.com/hyperledger/fabric/consensus/util"
	pb "github.com/hyperledger/fabric/protos"
	"golang.org/x/net/context"
	"github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("handlerManager")

type HandlerManager struct {
	handlerMap     *handlerMap
	outChan 		chan *util.Message
}

func NewChannelWithConsensus() (*HandlerManager,error){
    server,err := NewHandlerManager()
    if err != nil{
        return nil,fmt.Errorf("failed to create new channel service")
    }
    manLogger.Testf("create channel server is success")
    return server,nil
}


func NewHandlerManager () (handler *HandlerManager, err error){
	manLogger.Errorf("--create NewHandlerManager begin")
	handler = new(HandlerManager)
	handler.handlerMap = &handlerMap{m: make(map[pb.PeerID]MessageHandler)}
	handler.outChan = make(chan *util.Message)

	go func(){   //--------
		for msg := range handler.outChan { 
			manLogger.Debugf("----recv new  Msg from : %v",msg.Sender)
		}
	}()

	manLogger.Errorf("--create NewHandlerManager end")
	return handler,nil
}

//add a new peer
func (p *HandlerManager) connectPeer(endpoint *pb.PeerEndpoint) error {
	address := endpoint.Address
	manLogger.Debugf("Initiating Chat with peer address: %s", address)
	manLogger.Testf("Initiating Chat with peer address: %s", address)
	conn, err := NewPeerClientConnectionWithAddress(address)
	if err != nil {
		manLogger.Errorf("Error creating connection to peer address %s: %s", address, err)
		return err
	}
	manLogger.Testf("Connection address %s ok",address)
	serverClient := pb.NewPeerClient(conn)  
	ctx := context.Background()
	stream, err := serverClient.Chat(ctx)  
	if err != nil {
		manLogger.Errorf("Error establishing chat with peer address %s: %s", address, err)
		return err
	}

	manLogger.Testf("Established Chat with peer address: %s", address)
	manLogger.Debugf("Established Chat with peer address: %s", address)
	
	err = p.handleChat(ctx,stream,endpoint)
	stream.CloseSend() 
	if err != nil {
		manLogger.Errorf("Ending Chat with peer address %s due to error: %s", address, err)
		return err
	}
	return nil
}

func (p *HandlerManager) addHandler(endpoint *pb.PeerEndpoint,handler MessageHandler) error{
	manLogger.Testf("----addHandler-----size:%d: newAddr:%s", len(p.handlerMap.m),endpoint.Address)
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()    
	id := *endpoint.GetID()
	if _, ok := p.handlerMap.m[id]; ok == true {
		// Duplicate, return error
		manLogger.Testf("-----add new Handler in map error----")
		return newDuplicateHandlerError(handler)
	}
	p.handlerMap.m[id] = handler                      //------
	manLogger.Debugf("registered handler with key: %s", id)
	manLogger.Testf("-----add new Handler in map ok---")
	return nil
}

func (p *HandlerManager) deleteHandler(addresses string){

}

// Chat implementation of the the Chat bidi streaming RPC function
func (p *HandlerManager) handleChat(ctx context.Context, stream ChatStream,endpoint *pb.PeerEndpoint) error {
	deadline, ok := ctx.Deadline()
	manLogger.Debugf("Current context deadline = %s, ok = %v address = %s", deadline, ok)
	manLogger.Testf("Current context deadline = %s, ok = %v address = %s", deadline, ok)
	cHandler,err := NewChanHandler(p,stream,endpoint)
	if err != nil {
		return fmt.Errorf("Error creating handler during ConnectPeer initiation: %s", err)
	}
	defer cHandler.Stop() 
	manLogger.Testf("endpoint is nill :%t",(endpoint == nil))
	if endpoint != nil{
		err = p.addHandler(endpoint,cHandler)
		if err != nil{
			manLogger.Testf("Create stream handler to address %s failed err:%s",endpoint.Address,err)
			return err
		}
		manLogger.Testf("Create stream handler to address %s ok",endpoint.Address)
	}

	for {
		manLogger.Testf("read stream........")
		in, err := stream.Recv() 
		if err == io.EOF {
			manLogger.Debug("Received EOF, ending Chat")
			return nil
		}
		if err != nil {
			e := fmt.Errorf("Error during Chat, stopping handler: %s", err)
			manLogger.Error(e.Error())
			return e
		}
		err = cHandler.HandleMessage(in)  
		if err != nil {
			manLogger.Errorf("Error handling message: %s", err)
			return err
		}
	}
}

func (p *HandlerManager) Chat(stream pb.Peer_ChatServer) error {
    manLogger.Testf("chat server running.......")
	return p.handleChat(stream.Context(), stream, nil)
}

func (p *HandlerManager) RegisterHandler(endpoint *pb.PeerEndpoint) error  {
	manLogger.Testf("registered handler with address %s",endpoint.Address)
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()
	if _, ok := p.handlerMap.m[*endpoint.ID]; ok == true {
		// Duplicate, return error
		return  fmt.Errorf("Duplicate Handler address %s",endpoint.Address )
	}
	go p.connectPeer(endpoint)
    return nil
}

func (p *HandlerManager) DeregisterHandler(messageHandler MessageHandler) error {
	key, err := GetHandlerKey(messageHandler)
	if err != nil {
		return fmt.Errorf("Error deregistering handler: %s", err)
	}
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()
	if _, ok := p.handlerMap.m[*key]; !ok {
		// Handler NOT found
		return fmt.Errorf("Error deregistering handler, could not find handler with key: %s", key)
	}
	delete(p.handlerMap.m, *key)
	manLogger.Debugf("Deregistered handler with key: %s", key)
	return nil
}

func (p *HandlerManager) GetOutChannel() chan *util.Message {
	return p.outChan
}

//not used
func (p *HandlerManager) ProcessTransaction(ctx context.Context, tx *pb.Transaction) (response *pb.Response, err error) {
   return nil,nil
}

// Clone the handler map to avoid locking across SendMessage
func (p *HandlerManager) cloneHandlerMap(typ pb.PeerEndpoint_Type) map[pb.PeerID]MessageHandler {
	if typ != pb.PeerEndpoint_VALIDATOR {
		peerLogger.Errorf("not send msg to non validator")
		return nil
	}
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()
	clone := make(map[pb.PeerID]MessageHandler)
	for id, msgHandler := range p.handlerMap.m {
		//pb.PeerEndpoint_UNDEFINED collects all peers
		toPeerEndpoint, _ := msgHandler.To()
		//ignore endpoints that don't match type filter
		if typ != toPeerEndpoint.Type {
			continue
		}
		
		clone[id] = msgHandler
	}
	return clone
}

// Broadcast broadcast a message to each of the currently registered PeerEndpoints of given type
// Broadcast will broadcast to all registered PeerEndpoints if the type is PeerEndpoint_UNDEFINED
func (p *HandlerManager) Broadcast(msg *pb.Message, typ pb.PeerEndpoint_Type) []error {
	cloneMap := p.cloneHandlerMap(typ)
	errorsFromHandlers := make(chan error, len(cloneMap))
	var bcWG sync.WaitGroup

	start := time.Now()

	for _, msgHandler := range cloneMap {
		bcWG.Add(1)
		go func(msgHandler MessageHandler) {
			defer bcWG.Done()
			host, _ := msgHandler.To()
			t1 := time.Now()
			err := msgHandler.SendMessage(msg)
			if err != nil {
				toPeerEndpoint, _ := msgHandler.To()
				errorsFromHandlers <- fmt.Errorf("Error broadcasting msg (%s) to PeerEndpoint (%s): %s", msg.Type, toPeerEndpoint, err)
			}
			peerLogger.Debugf("Sending %d bytes to %s took %v", len(msg.Payload), host.Address, time.Since(t1))

		}(msgHandler)

	}
	bcWG.Wait()
	close(errorsFromHandlers)
	var returnedErrors []error
	for err := range errorsFromHandlers {
		returnedErrors = append(returnedErrors, err)
	}

	elapsed := time.Since(start)
	peerLogger.Debugf("Broadcast took %v", elapsed)

	return returnedErrors
}

func (p *HandlerManager) getMessageHandler(receiverHandle *pb.PeerID) (MessageHandler, error) {
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()
	msgHandler, ok := p.handlerMap.m[*receiverHandle]
	if !ok {
		return nil, fmt.Errorf("Message handler not found for receiver %s", receiverHandle.Name)
	}
	return msgHandler, nil
}

// Unicast sends a message to a specific peer.
func (p *HandlerManager) Unicast(msg *pb.Message, receiverHandle *pb.PeerID) error {
	msgHandler, err := p.getMessageHandler(receiverHandle)
	if err != nil {
		return err
	}
	err = msgHandler.SendMessage(msg)
	if err != nil {
		toPeerEndpoint, _ := msgHandler.To()
		return fmt.Errorf("Error unicasting msg (%s) to PeerEndpoint (%s): %s", msg.Type, toPeerEndpoint, err)
	}
	return nil
}
