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
    "google.golang.org/grpc"
    "github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("handlerManager")

type chanHandlerMap struct{
	sync.RWMutex
	m  map[pb.PeerID]*ChanHandler 		//connected peers
	u  map[pb.PeerID]*pb.PeerEndpoint   //Not connected peers
}


type HandlerManager struct {
	handlerMap      *chanHandlerMap  			
	peerEndpoint    *pb.PeerEndpoint    //self peer info
	outChan 		chan *util.Message
}

func NewChannelWithConsensus(ep *pb.PeerEndpoint) (*HandlerManager,error){
	epoint,err := ChangeAddress(ep)
	if err != nil{
		return nil,err
	}
    server,err := NewHandlerManager(epoint)
    if err != nil{
        return nil,fmt.Errorf("failed to create new channel service error: %s",err)
    }
    manLogger.Warningf("create channel with consensus server is success")
    return server,nil
}

func NewHandlerManager (selfPoint *pb.PeerEndpoint) (handler *HandlerManager, err error){
	handler = new(HandlerManager)
	handler.handlerMap = &chanHandlerMap{m: make(map[pb.PeerID]*ChanHandler),u:make(map[pb.PeerID]*pb.PeerEndpoint)}
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
			handler.handlerMap.RLock()
			logger.Infof("peer %v reconnection,connected size:%d,not connected size:%d",selfPoint.ID,len(handler.handlerMap.m),len(handler.handlerMap.u)) 
			if len(handler.handlerMap.u) >0 {
			 	for _,v := range handler.handlerMap.u{
			 		logger.Infof("peer %v reconnection peer address:%s",selfPoint.ID,v.Address)
			 		go handler.RegisterNewHandler(v.Address)
			 	}
			}
            handler.handlerMap.RUnlock()
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
	manLogger.Errorf("Error self drop connection from %v to %s", p.peerEndpoint.ID,address)
	conn.Close()
}

//connect a new peer
func (p *HandlerManager) connectPeer(address string) error {
	manLogger.Errorf("Initiating Chat with peer address %s", address)
	conn, err := NewPeerClientConnectionWithAddress(address)
	if err != nil {
		manLogger.Errorf("Error creating connection to peer address:%s: %s", address, err)
		return err
	}
	defer p.closeConnect(conn,address)
	manLogger.Warningf("Connection iaddress:%s ok",address)
	serverClient := pb.NewPeerClient(conn)  
	ctx := context.Background()
	stream, err := serverClient.Chat(ctx)  
	if err != nil {
		manLogger.Errorf("Error establishing chat with peer address:%s : %s", address, err)
		return err
	}
    
	manLogger.Warningf("Established Chat with peer address:%s", address)
	
	err = p.handleChat(ctx,stream,true)
	stream.CloseSend() 
	if err != nil {
		manLogger.Errorf("Ending Chat with peer address:%s due to error: %s", address, err)
		return err
	}
	return nil
}

// Chat implementation of the the Chat bidi streaming RPC function
func (p *HandlerManager) handleChat(ctx context.Context, stream ChatStream,initiatedStream bool) error {
	var err error
	deadline, ok := ctx.Deadline()
	manLogger.Warningf("Current context deadline = %s, ok = %v", deadline, ok)

	cHandler,err := NewChanHandler(p,stream,initiatedStream)
	if err != nil{
		return fmt.Errorf("Error creating handler during handleChat initiation: %s", err)
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
    err := p.handleChat(stream.Context(), stream, false)
    manLogger.Warningf("chat server Stop.......err:%s",err)
    return err
}

func (p *HandlerManager) GetHandlerByKey(id *pb.PeerID) *ChanHandler{
	p.handlerMap.RLock()
	defer p.handlerMap.RUnlock()    
	if handler, ok := p.handlerMap.m[*id]; ok == true {
		return handler
	}
	return nil
}

func (p *HandlerManager) RegisterNewHandler(address string){
	manLogger.Warningf("registered new peer handler with address:%s",address)
	go p.connectPeer(address)
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
	delete(p.handlerMap.u, key)
	peerLogger.Errorf("RegisterHandler reconnection map1-size:%d map2-size:%d del:%v",len(p.handlerMap.m),len(p.handlerMap.u),key)
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
	p.handlerMap.u[key] = &toPeerEndpoint
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
