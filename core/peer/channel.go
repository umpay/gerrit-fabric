package peer

import (
    "fmt"
    "time"
    "strings"
    "sync"
    "io"
    "github.com/hyperledger/fabric/consensus/util"
    cutil "github.com/hyperledger/fabric/core/util"
    "github.com/golang/protobuf/proto"
    pb "github.com/hyperledger/fabric/protos"
    "github.com/spf13/viper"
    "golang.org/x/net/context"
    "github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("handlerManager")

type HandlerManager struct {
	handlerMap      *handlerMap
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
        return nil,fmt.Errorf("failed to create new channel service")
    }
    manLogger.Testf("create channel server is success")
    return server,nil
}

func NewHandlerManager (selfPoint *pb.PeerEndpoint) (handler *HandlerManager, err error){
	manLogger.Errorf("--create NewHandlerManager begin")
	handler = new(HandlerManager)
	handler.handlerMap = &handlerMap{m: make(map[pb.PeerID]MessageHandler)}
	handler.outChan = make(chan *util.Message)
	if selfPoint == nil{
		return nil,fmt.Errorf("self Point is nill")
	}
	handler.peerEndpoint = selfPoint

	go func(){   //--------
		for msg := range handler.outChan { 
	           manLogger.Testf("----self:%v recv new  Msg from : %v  mapsize:%d",selfPoint.ID,msg.Sender,len(handler.handlerMap.m))
		}
	}()

	manLogger.Errorf("--create NewHandlerManager end")
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
    manLogger.Testf("---self point :id %v addr:%s addrq:%s type %s",selfPoint.ID,selfPoint.Address,ep.Address,selfPoint.Type)
    return selfPoint,nil
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

func (p *HandlerManager) sendHello(handler MessageHandler) {
	logger.Errorf("--SendHello--send helloMessage begin---")
	if handler == nil{
		manLogger.Errorf("handler is nil")
	}
	if p.peerEndpoint == nil{
		manLogger.Errorf("self Endpoint is nil")
		return
	}
	helloMessage := &pb.HelloMessage{PeerEndpoint: p.peerEndpoint}
	data, err := proto.Marshal(helloMessage)
	if err != nil {
		manLogger.Errorf("Error marshalling HelloMessage: %s", err)
		return
	}
	// Need to sign the Discovery Hello message
	newDiscoveryHelloMsg := &pb.Message{Type: pb.Message_DISC_HELLO, Payload: data, Timestamp: cutil.CreateUtcTimestamp()}
	//err = p.signMessageMutating(newDiscoveryHelloMsg)

	if err := handler.SendMessage(newDiscoveryHelloMsg); err != nil {
		manLogger.Errorf("Error creating new Peer Handler, error returned sending %s: %s", pb.Message_DISC_HELLO, err)
		return
	}
	manLogger.Errorf("--SendHello--send helloMessage ok")
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

	if endpoint != nil{
	 	manLogger.Testf("---handleChat--..RegisterHandler ... id %v addr:%s",endpoint.ID,endpoint.Address)
		err = p.RegisterHandler(cHandler)
		if err != nil{
			return fmt.Errorf("Error register failed error %s",err)
		}
		p.sendHello(cHandler)  //send hello msg
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

func (p *HandlerManager) GetHandlerByKey(id *pb.PeerID) MessageHandler{
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()    
	if handler, ok := p.handlerMap.m[*id]; ok == true {
		return handler
	}
	return nil
}

func (p *HandlerManager) RegisterNewHandler(endpoint *pb.PeerEndpoint) error  {
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

func (p *HandlerManager) RegisterHandler(messageHandler MessageHandler) error {
	key, err := GetHandlerKey(messageHandler)
	if err != nil {
		return fmt.Errorf("Error registering handler: %s", err)
	}
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()
	if _, ok := p.handlerMap.m[*key]; ok == true {
		// Duplicate, return error
		return newDuplicateHandlerError(messageHandler)
	}
	p.handlerMap.m[*key] = messageHandler
	peerLogger.Debugf("registered handler with key: %s handler size:%d",key, len(p.handlerMap.m))
	peerLogger.Errorf("registered handler with key: %s handler size:%d", key,len(p.handlerMap.m))
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
			peerLogger.Errorf("--Broadcast--Sending %d bytes to %s took %v", len(msg.Payload), host.Address, time.Since(t1))

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
	peerLogger.Errorf("---Unicast-receiverHandleo %v", receiverHandle)
	msgHandler, err := p.getMessageHandler(receiverHandle)
	if err != nil {
		return err
	}
	err = msgHandler.SendMessage(msg)
	if err != nil {
		toPeerEndpoint, _ := msgHandler.To()
		return fmt.Errorf("Error unicasting msg (%s) to PeerEndpoint (%s): %s", msg.Type, toPeerEndpoint, err)
	}

	toPeerEndpoint, _ := msgHandler.To()
	peerLogger.Errorf("---Unicast-Sending %d bytes to %s", len(msg.Payload), toPeerEndpoint.Address)
	return nil
}
