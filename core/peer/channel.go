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
    "google.golang.org/grpc"
    "golang.org/x/net/context"
    "github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("handlerManager")

type chanHandlerMap struct{
	sync.RWMutex
	m map[pb.PeerID]*ChanHandler
}

type HandlerManager struct {
	handlerMap      *chanHandlerMap  			 //connected peers
	unHandlerMap	map[pb.PeerID]*pb.PeerEndpoint   //Not connected peers
	peerEndpoint    *pb.PeerEndpoint    		//self 
	outChan 	chan *util.Message
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
    manLogger.Warningf("create channel with consensus server is success")
    return server,nil
}

func NewHandlerManager (selfPoint *pb.PeerEndpoint) (handler *HandlerManager, err error){
	handler = new(HandlerManager)
	handler.handlerMap = &chanHandlerMap{m: make(map[pb.PeerID]*ChanHandler)}
	handler.unHandlerMap = make(map[pb.PeerID]*pb.PeerEndpoint)
	handler.outChan = make(chan *util.Message,1000)
	if selfPoint == nil{
		return nil,fmt.Errorf("self Point is nill")
	}
	handler.peerEndpoint = selfPoint
	go func (){
		tickChan := time.NewTicker(time.Second * 5).C
		logger.Infof("Starting Peer reconnection service")
		for {
			 <-tickChan
			logger.Infof("peer %v reconnection map size:%d",selfPoint.ID,len(handler.unHandlerMap))
			 if len(handler.unHandlerMap) >0 {
			 	for k,v := range handler.unHandlerMap{
			 		logger.Infof("peer %v reconnection peer id:%v",selfPoint.ID,k)
			 		go handler.RegisterNewHandler(v)
			 	}
			 }
		}
	}()
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

func (p *HandlerManager) closeConnect(conn *grpc.ClientConn,address string){
	manLogger.Errorf("Error drop connection from %v to %s", p.peerEndpoint.ID,address)
	conn.Close()
}

//connect a new peer
func (p *HandlerManager) connectPeer(endpoint *pb.PeerEndpoint) error {
	manLogger.Infof("Initiating Chat with peer id: %v", endpoint.ID)
	conn, err := NewPeerClientConnectionWithAddress(endpoint.Address)
	if err != nil {
		manLogger.Errorf("Error creating connection to peer id:%s: %s", endpoint.ID, err)
		return err
	}
	defer p.closeConnect(conn,endpoint.Address)
	manLogger.Warningf("Connection id:%v ok",endpoint.ID)
	serverClient := pb.NewPeerClient(conn)  
	ctx := context.Background()
	stream, err := serverClient.Chat(ctx)  
	if err != nil {
		manLogger.Errorf("Error establishing chat with peer id:%v : %s", endpoint.ID, err)
		return err
	}
	manLogger.Warningf("Established Chat with peer id: %v", endpoint.ID)
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
	var err error
	deadline, ok := ctx.Deadline()
	manLogger.Warningf("Current context deadline = %s, ok = %v", deadline, ok)
	cHandler,err := NewChanHandler(p,stream,endpoint)
	if err != nil{
		manLogger.Errorf("Create Handler from %v to %v failed,error %s",p.peerEndpoint.ID,endpoint.ID,err)
		return err
	}
	defer cHandler.Stop()
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
    manLogger.Warningf("chat server running.......")
    err := p.handleChat(stream.Context(), stream, nil)
    manLogger.Warningf("chat server Stop.......err:%s",err)
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
	manLogger.Warningf("registered new peer handler with id:%v",endpoint.ID)
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
	toPeerEndpoint,err := messageHandler.To()
	if err != nil {
		return fmt.Errorf("Error getting messageHandler PeerEndpoint: %s", err)
	}
	key := *toPeerEndpoint.ID
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()
	if _, ok := p.handlerMap.m[key]; ok == true {
		// Duplicate, return error
		return fmt.Errorf("Error Duplicate Handler from %v to %v",p.peerEndpoint.ID,key ) 
	}
	p.handlerMap.m[key] = messageHandler
	delete(p.unHandlerMap, key)
	peerLogger.Errorf("RegisterHandler reconnection map1 size:%d map2 size:%d del:%v",len(p.handlerMap.m),len(p.unHandlerMap),key)
	peerLogger.Warningf("Registered handler from %v to %v,handler size:%d",p.peerEndpoint.ID,key, len(p.handlerMap.m))
	return nil
}

func (p *HandlerManager) DeregisterHandler(messageHandler *ChanHandler) error {
	toPeerEndpoint,err := messageHandler.To()
	if err != nil {
		return fmt.Errorf("Error getting messageHandler PeerEndpoint: %s", err)
	}
	key := *toPeerEndpoint.ID
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()
	if _, ok := p.handlerMap.m[key]; !ok {
		// Handler NOT found
		return fmt.Errorf("Error deregistering handler, could not find handler with key: %s", key)
	}
	
	delete(p.handlerMap.m, *toPeerEndpoint.ID)
	p.unHandlerMap[key] = &toPeerEndpoint
	peerLogger.Errorf("DeregisterHandler reconnection map1 size:%d map2 size:%d add:%v",len(p.handlerMap.m),len(p.unHandlerMap),key)
	manLogger.Warningf("Deregistered handler from %v to %v,handler size:%d",p.peerEndpoint.ID,key,len(p.handlerMap.m))
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
	return nil
}
