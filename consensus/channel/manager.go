package channel

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/consensus/util"
	pb "github.com/hyperledger/fabric/protos"
	"golang.org/x/net/context"
	"github.com/op/go-logging"
)
var manLogger = logging.MustGetLogger("hanadlerManager")

type MessageHandler interface {  
	HandleMessage(msg *pb.Message) error 
	SendMessage(msg *pb.Message) error  
	To() (pb.PeerEndpoint, error)
	Stop() error
}

// ChatStream interface supported by stream between Peers
type ChatStream interface {
	Send(*pb.Message) error
	Recv() (*pb.Message, error)
}

type MessageHandlerCoordinator interface {
	RegisterHandler(endpoint *pb.PeerEndpoint) error  
	DeregisterHandler(messageHandler MessageHandler) error
	Broadcast(*pb.Message, pb.PeerEndpoint_Type) []error
	Unicast(*pb.Message, *pb.PeerID) error
	GetOutChannel() <-chan *util.Message
}

type handlerMap struct {
	sync.RWMutex
	m map[pb.PeerID]MessageHandler
}

type HanadlerManager struct {
	handlerMap     *handlerMap
	outChan  chan *util.Message
}

func NewHanadlerManager (endpoints []*pb.PeerEndpoint) (handler *HanadlerManager, err error){
	handler = new(HanadlerManager)
	handler.handlerMap = &handlerMap{m: make(map[pb.PeerID]MessageHandler)}
	handler.outChan = make(chan *util.Message)

	selfPeerId, err := GetPeerEndpoint()   //get self peerInfo 
	if err != nil{
		manLogger.Errorf("Failed to obtain peer endpoint, %v", err)
		return nil,err
	}

	go func(){   //--------
		for msg := range d.outChan { 
			manLogger.Debugf("----recv Msg from : %v",msg.Sender)
		}
	}()

	for _, endpoint := range endpoints {
		if endpoint.GetID() == selfPeerId.GetID() {
			manLogger.Debugf("Skipping own address: %v",selfPeerId.Address)
			continue
		}
		go handler.ConnectPeer(endpoint)
	}
	return handler
}

func (p *HanadlerManager) addPeer(endpoint *pb.PeerEndpoint,handler MessageHandler) error{
	p.handlerMap.Lock()
	defer p.handlerMap.Unlock()    
	id := endpoint.GetID()
	if _, ok := p.handlerMap.m[id]; ok == true {
		// Duplicate, return error
		return newDuplicateHandlerError(messageHandler)
	}
	p.handlerMap.m[id] = handler                      //------
	manLogger.Debugf("registered handler with key: %s", id)
	return nil
}

//add a new peer
func (p *HanadlerManager) ConnectPeer(endpoint *pb.PeerEndpoint) error {
	address := endpoint.Address
	manLogger.Debugf("Initiating Chat with peer address: %s", address)
	conn, err := NewPeerClientConnectionWithAddress(address) //创建一个连接到指定地址
	if err != nil {
		manLogger.Errorf("Error creating connection to peer address %s: %s", address, err)
		return err
	}
	serverClient := pb.NewPeerClient(conn)  
	ctx := context.Background()
	stream, err := serverClient.Chat(ctx)  
	if err != nil {
		manLogger.Errorf("Error establishing chat with peer address %s: %s", address, err)
		return err
	}
	manLogger.Debugf("Established Chat with peer address: %s", address)
	
	//-----------------------------------
	err = p.handleChat(ctx,stream)
	stream.CloseSend()  //关闭流
	if err != nil {
		manLogger.Errorf("Ending Chat with peer address %s due to error: %s", address, err)
		return err
	}
	return nil
}

func (p *HanadlerManager) deletePeer(addresses string){

}

// Chat implementation of the the Chat bidi streaming RPC function
func (p *HanadlerManager) handleChat(ctx context.Context, stream ChatStream) error {
	deadline, ok := ctx.Deadline()
	peerLogger.Debugf("Current context deadline = %s, ok = %v", deadline, ok)

	cHandler,err := NewChatHandler(p,stream)
	if err != nil {
		return fmt.Errorf("Error creating handler during ConnectPeer initiation: %s", err)
	}
	err = p.addPeer(endpoint,cHandler)
	if err != nil{
		peerLogger.Errorf("Error: %s", err)
		return err
	}
	defer cHandler.Stop() 
	for {
		in, err := stream.Recv() 
		if err == io.EOF {
			peerLogger.Debug("Received EOF, ending Chat")
			return nil
		}
		if err != nil {
			e := fmt.Errorf("Error during Chat, stopping handler: %s", err)
			peerLogger.Error(e.Error())
			return e
		}
		err = cHandler.HandleMessage(in)  
		if err != nil {
			peerLogger.Errorf("Error handling message: %s", err)
			//return err
		}
	}
}

func (p *HanadlerManager) RegisterHandler(endpoint *pb.PeerEndpoint) error {
	go p.ConnectPeer(endpoint)
}

func (p *HanadlerManager) DeregisterHandler(messageHandler MessageHandler) error {
	key, err := getHandlerKey(messageHandler)
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
	peerLogger.Debugf("Deregistered handler with key: %s", key)
	return nil
}

func (p *HanadlerManager) GetOutChannel() <-chan *util.Message {
	return p.consenterChan
}


func (p *HanadlerManager) Broadcast(*pb.Message, pb.PeerEndpoint_Type) []error{

}

func (p *HanadlerManager) Unicast(*pb.Message, *pb.PeerID) error{

}

