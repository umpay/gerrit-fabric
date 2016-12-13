package peer
import (
    "fmt"
    "sync"
    "github.com/hyperledger/fabric/consensus/util"
    cutil "github.com/hyperledger/fabric/core/util"
    pb "github.com/hyperledger/fabric/protos"
    "github.com/golang/protobuf/proto"
    "github.com/op/go-logging"
)
var logger = logging.MustGetLogger("ChanHandler")

type MessageHandlerCoordinatorC interface {
        RegisterHandler(messageHandler *ChanHandler) error
        DeregisterHandler(messageHandler *ChanHandler) error
        Broadcast(*pb.Message, pb.PeerEndpoint_Type) []error
        Unicast(*pb.Message, *pb.PeerID) error
        GetOutChannel() chan *util.Message
        GetHandlerByKey(*pb.PeerID) *ChanHandler
        GetSelfPeerEndpoint() *pb.PeerEndpoint
}


//channel handler
type ChanHandler struct {
	chatMutex             sync.Mutex
	Coordinator           MessageHandlerCoordinatorC
	ToPeerEndpoint        *pb.PeerEndpoint
	registered            bool
	ChatStream            ChatStream 
	consenterChan 	      chan *util.Message
}


func NewChanHandler(coord MessageHandlerCoordinatorC, stream ChatStream,point *pb.PeerEndpoint) (*ChanHandler) {
	d := &ChanHandler{
		ChatStream:      stream,
		Coordinator:     coord,
		registered:  	 false,
	}
    d.ToPeerEndpoint = point
	d.consenterChan = make(chan *util.Message, 1000)
	go func (){
		outChan := d.Coordinator.GetOutChannel()
		for msg := range d.consenterChan { 
			outChan <-msg
		}
	}()

	return d
}

// HandleMessage handles the incoming Fabric messages for the Peer
func (d *ChanHandler) HandleMessage(msg *pb.Message) error { 
	if msg.Type == pb.Message_CONSENSUS { 
		senderPE, _ := d.To()
		select {
		case d.consenterChan <- &util.Message{
				Msg:    msg,
				Sender: senderPE.ID,
			}:
			return nil
		default:
			err:= fmt.Errorf("Message channel for %v full, rejecting", senderPE.ID)
			logger.Errorf("Failed to queue consensus message because: %v", err)
			return err
		}
	}else if  msg.Type == pb.Message_DISC_HELLO{
		helloMessage := &pb.HelloMessage{}
		err := proto.Unmarshal(msg.Payload, helloMessage)
		if err != nil {
			return fmt.Errorf("Error unmarshalling HelloMessage: %s", err)
		}
		helloPeerEndpoint :=helloMessage.PeerEndpoint
		logger.Errorf("--HandleMessage---sendId:%v registered:%t", helloPeerEndpoint.ID,d.registered)
		if d.registered == false{
			handler := d.Coordinator.GetHandlerByKey(helloPeerEndpoint.ID)
			if handler != nil{
				//delete
				err = d.deregister()
				if err != nil{
					logger.Testf("Deregister handler id: %v,Error %s", helloPeerEndpoint.ID,err)
				}else{
					logger.Testf("Deregister handler id: %v ok", helloPeerEndpoint.ID)
				}
			}
			//add			
			d.ToPeerEndpoint = helloPeerEndpoint
			err = d.Coordinator.RegisterHandler(d)
			if err != nil{
				logger.Errorf("-----Register handler id: %v registered:%t,errror %s", helloPeerEndpoint.ID,d.registered,err)
				return err
			}else{
			    logger.Testf("Register handler id: %v registered:%t ok",helloPeerEndpoint.ID,d.registered)
			}
		}
		return err
	}

	return fmt.Errorf("Did not handle message of type %s, passing on to next MessageHandler", msg.Type)
}

func (d *ChanHandler) SendMessage(msg *pb.Message) error {
	//make sure Sends are serialized. Also make sure everyone uses SendMessage
	//instead of calling Send directly on the grpc stream
	d.chatMutex.Lock()
	defer d.chatMutex.Unlock()
	logger.Debugf("Sendding message to stream of type: %s  to:%v", msg.Type,d.ToPeerEndpoint.ID)
	err := d.ChatStream.Send(msg)
	if err != nil {
		return fmt.Errorf("Error Sending message through ChatStream: %s", err)
	}
	return nil
}

func (d *ChanHandler) To() (pb.PeerEndpoint, error){
  	if d.ToPeerEndpoint == nil {
		return pb.PeerEndpoint{}, fmt.Errorf("No peer endpoint for handler")
	}
	return *(d.ToPeerEndpoint), nil
}

func (d *ChanHandler) Register() {
	d.registered = true

	senderPE, _ := d.To()
	logger.Testf("----Register handler to %v ok",senderPE.ID)
}

func (d *ChanHandler) deregister() error {
	var err error
	if d.registered {
		senderPE, _ := d.To()
		err = d.Coordinator.DeregisterHandler(d)
		if err != nil{
			logger.Errorf("deregister handler to %v,error %s",senderPE.ID,err)
		}else{
			d.registered = false
			logger.Testf("deregister handler to %v ok",senderPE.ID)
		}
	}
	return err
}

// Stop stops this handler, which will trigger the Deregister from the MessageHandlerCoordinator.
func (d *ChanHandler) Stop() error {
	// Deregister the handler
	senderPE, _ := d.To()
	logger.Errorf("------stop handler to %v registered :%t",senderPE.ID,d.registered)
	err := d.deregister()
	if err != nil {
		return fmt.Errorf("Error stopping MessageHandler: %s", err)
	}
	return nil
}


func (d *ChanHandler) SendHello() error{
	senderPE, _ := d.To()
	selfPoint := d.Coordinator.GetSelfPeerEndpoint()
	if selfPoint == nil{
		return fmt.Errorf("self Endpoint is nil")
	}
	helloMessage := &pb.HelloMessage{PeerEndpoint: selfPoint}
	data, err := proto.Marshal(helloMessage)
	if err != nil {
		return fmt.Errorf("Error marshalling HelloMessage: %s", err)
	}
	// Need to sign the Discovery Hello message
	newDiscoveryHelloMsg := &pb.Message{Type: pb.Message_DISC_HELLO, Payload: data, Timestamp: cutil.CreateUtcTimestamp()}
	//err = p.signMessageMutating(newDiscoveryHelloMsg)

	if err := d.SendMessage(newDiscoveryHelloMsg); err != nil {
		manLogger.Errorf("%v Send %s to %v,error %s",selfPoint.ID,pb.Message_DISC_HELLO,senderPE.ID,err)
		return err
	}
	manLogger.Errorf("%v Send %s to %v ok",selfPoint.ID,pb.Message_DISC_HELLO,senderPE.ID)
	return nil
}
