package peer

import (
    "fmt"
    "time"
    "strings"
    "sync"
    "io"
    "github.com/hyperledger/fabric/consensus/util"
    pb "github.com/hyperledger/fabric/protos"
    "github.com/spf13/viper"
    "golang.org/x/net/context"
    "github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("handlerManager")

type chanHandlerMap struct{
	sync.RWMutex
	m map[pb.PeerID]*ChanHandler
}

type HandlerManager struct {
	handlerMap      *chanHandlerMap
	peerEndpoint    *pb.PeerEndpoint  //self
	outChan 		chan *util.Message
}

func NewChannelWithConsensus(ep *pb.PeerEndpoint) (*HandlerManager,error){
	epoint,err := ChangeAddress(ep)
	if err != nil{
		return nil,err
	}
    server,err := NewHandlerManager(epoint)
    if err != nil{
        return nil,fmt.Errorf("failed to create new channel service error: %",err)
    }
    manLogger.Testf("create channel with consensus server is success")
    return server,nil
}

func NewHandlerManager (selfPoint *pb.PeerEndpoint) (handler *HandlerManager, err error){
	handler = new(HandlerManager)
	handler.handlerMap = &chanHandlerMap{m: make(map[pb.PeerID]*ChanHandler)}
	handler.outChan = make(chan *util.Message,1000)
	if selfPoint == nil{
		return nil,fmt.Errorf("self Point is nill")
	}
	handler.peerEndpoint = selfPoint
	return handler,nil
}

func ChangeAddress(ep *pb.PeerEndpoint) (*pb.PeerEndpoint,error){
	if ep == nil{
		return nil,fmt.Errorf("Point is nill")
	}
	chanelAddress := viper.GetString("peer.consensusAddress")
	selfPoint :=&pb.PeerEndpoint{
			ID:       ep.ID,
			Address:  fmt.Sprintf("%s:%s",strings.Split(ep.Address,":")[0],strings.Split(chanelAddress,":")[1]),
			Type:     ep.Type,
			PkiID:    ep.PkiID,
		}
    return selfPoint,nil
}

//connect a new peer
func (p *HandlerManager) connectPeer(endpoint *pb.PeerEndpoint) error {
	manLogger.Debugf("Initiating Chat with peer id: %v", endpoint.ID)
	manLogger.Testf("Initiating Chat with peer id: %v", endpoint.ID)
	conn, err := NewPeerClientConnectionWithAddress(endpoint.Address)
	if err != nil {
		manLogger.Errorf("Error creating connection to peer id:%s: %s", endpoint.ID, err)
		return err
	}
	manLogger.Testf("Connection id:%v ok",endpoint.ID)
	serverClient := pb.NewPeerClient(conn)  
	ctx := context.Background()
	stream, err := serverClient.Chat(ctx)  
	if err != nil {
		manLogger.Errorf("Error establishing chat with peer id:%v : %s", endpoint.ID, err)
		return err
	}
    
	manLogger.Testf("Established Chat with peer id: %v", endpoint.ID)
	manLogger.Debugf("Established Chat with peer id: %v", endpoint.ID)
	
	err = p.handleChat(ctx,stream,endpoint)
	stream.CloseSend() 
	if err != nil {
		manLogger.Errorf("Ending Chat with peer id:%v due to error: %s", endpoint.ID, err)
		return err
	}
	return nil
}

// Chat implementation of the the Chat bidi streaming RPC function
func (p *HandlerManager) handleChat(ctx context.Context, stream ChatStream,endpoint *pb.PeerEndpoint) error {
	deadline, ok := ctx.Deadline()
	manLogger.Debugf("Current context deadline = %s, ok = %v", deadline, ok)
	manLogger.Testf("Current context deadline = %s, ok = %v", deadline, ok)
	cHandler := NewChanHandler(p,stream,endpoint)
	defer cHandler.Stop()
    var err error	
	if endpoint != nil{
		err = p.RegisterHandler(cHandler)
		if err != nil{
			return err
		}
		manLogger.Testf("RegisterHandler %v add handler id %v ok",p.peerEndpoint.ID,endpoint.ID)
		cHandler.SendHello() //send hello msg to server 
	}
	for {
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
    err := p.handleChat(stream.Context(), stream, nil)
    manLogger.Errorf("----chat server Stop.......err:%s",err)
    return err
}

func (p *HandlerManager) GetHandlerByKey(id *pb.PeerID) *ChanHandler{
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()    
	if handler, ok := p.handlerMap.m[*id]; ok == true {
		return handler
	}
	return nil
}

func (p *HandlerManager) RegisterNewHandler(endpoint *pb.PeerEndpoint){
	manLogger.Testf("registered new peer handler with id:%v",endpoint.ID)
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()
	if _, ok := p.handlerMap.m[*endpoint.ID]; ok == true {
		peerLogger.Errorf("Error Duplicate Handler from %v to %v",p.peerEndpoint.ID,endpoint.ID ) 
	}else{
		go p.connectPeer(endpoint)
	}
    return
}

func (p *HandlerManager) RegisterHandler(messageHandler *ChanHandler) error {
	key, err := GetPeerIDByHandler(messageHandler)
	if err != nil {
		return fmt.Errorf("Error registering handler: %s", err)
	}
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()
	if _, ok := p.handlerMap.m[*key]; ok == true {
		// Duplicate, return error
		return fmt.Errorf("Error Duplicate Handler from %v to %v",p.peerEndpoint.ID,key ) 
	}
	messageHandler.Register()   //set true
	p.handlerMap.m[*key] = messageHandler
	peerLogger.Debugf("registered handler with key: %s handler size:%d",key, len(p.handlerMap.m))
	peerLogger.Errorf("registered handler with key: %s handler size:%d", key,len(p.handlerMap.m))
	return nil
}

func (p *HandlerManager) DeregisterHandler(messageHandler *ChanHandler) error {
	key, err := GetPeerIDByHandler(messageHandler)
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
	manLogger.Debugf("Deregistered handler with key: %s handler size:%d", key,len(p.handlerMap.m))
	manLogger.Errorf("Deregistered handler with key: %s handler size:%d", key,len(p.handlerMap.m))
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
func (p *HandlerManager) cloneHandlerMap(typ pb.PeerEndpoint_Type) map[pb.PeerID]*ChanHandler {
	if typ != pb.PeerEndpoint_VALIDATOR {
		peerLogger.Errorf("not send msg to non validator")
		return nil
	}
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()
	clone := make(map[pb.PeerID]*ChanHandler)
        var index int
	for id, msgHandler := range p.handlerMap.m {
		//pb.PeerEndpoint_UNDEFINED collects all peers
		toPeerEndpoint, _ := msgHandler.To()
		//ignore endpoints that don't match type filter
		if typ != toPeerEndpoint.Type {
			continue
		}
                peerLogger.Errorf("self:%v index:%d to%v registerd:%t",p.peerEndpoint.ID,index,toPeerEndpoint.ID)	
 		index += 1
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
		go func(msgHandler *ChanHandler) {
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
func (p *HandlerManager) GetSelfPeerEndpoint() *pb.PeerEndpoint{
	return p.peerEndpoint
}

func GetPeerIDByHandler(peerMessageHandler *ChanHandler) (*pb.PeerID, error) {
	peerEndpoint, err := peerMessageHandler.To()
	if err != nil {
		return &pb.PeerID{}, fmt.Errorf("Error getting messageHandler key: %s", err)
	}
	return peerEndpoint.ID, nil
}

func (p *HandlerManager) getMessageHandler(receiverHandle *pb.PeerID) (*ChanHandler, error) {
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
	peerLogger.Errorf("---Unicast02 msg Type:%s to:%v----mapsize:%d", msg.Type, receiverHandle,len(p.handlerMap.m))
	return nil
}
