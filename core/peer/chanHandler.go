package peer
import (
    "fmt"
    "sync"
    "github.com/hyperledger/fabric/consensus/util"
    pb "github.com/hyperledger/fabric/protos"
    "github.com/golang/protobuf/proto"
    "github.com/op/go-logging"
)
var logger = logging.MustGetLogger("ChanHandler")

type MessageHandlerCoordinatorC interface {
        RegisterHandler(messageHandler MessageHandler) error
        DeregisterHandler(messageHandler MessageHandler) error
        Broadcast(*pb.Message, pb.PeerEndpoint_Type) []error
        Unicast(*pb.Message, *pb.PeerID) error
        GetOutChannel() chan *util.Message
        GetHandlerByKey(*pb.PeerID) MessageHandler
}


//channel handler
type ChanHandler struct {
	MessageHandler
	chatMutex             sync.Mutex
	Coordinator           MessageHandlerCoordinatorC
	ToPeerEndpoint        *pb.PeerEndpoint
	registered            bool
	ChatStream            ChatStream 
	consenterChan 	      chan *util.Message
}


func NewChanHandler(coord MessageHandlerCoordinatorC, stream ChatStream,point *pb.PeerEndpoint) (MessageHandler, error) {
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
	
	return d,nil
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
		if d.registered == false{
			handler := d.Coordinator.GetHandlerByKey(helloPeerEndpoint.ID)
			if handler != nil{
				//delete
				err = d.deregister()
				logger.Testf("Deregister handler address: %s", helloPeerEndpoint.Address)
			}
			//add			
			d.ToPeerEndpoint = helloPeerEndpoint
			logger.Testf("Register handler address: %s", helloPeerEndpoint.Address)
			err = d.Coordinator.RegisterHandler(d)
			if err != nil{
				logger.Testf("Register handler address: %s errror %s", helloPeerEndpoint.Address,err)
			}else{
			 d.registered = true
			}
			logger.Testf("Register handler address: %s ok  d.registered:%t",helloPeerEndpoint.Address,d.registered)
		}else{
			logger.Testf("Register handler HAVE address: %s ok  d.registered:%t",helloPeerEndpoint.Address,d.registered)
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

func (d *ChanHandler) IsRegister() bool{
	return d.registered
}

func (d *ChanHandler) deregister() error {
	var err error
	if d.registered {
		senderPE, _ := d.To()
		logger.Testf("deregister handler to %s ok",senderPE.Address)
		err = d.Coordinator.DeregisterHandler(d)
		d.registered = false
	}
	return err
}

// Stop stops this handler, which will trigger the Deregister from the MessageHandlerCoordinator.
func (d *ChanHandler) Stop() error {
	// Deregister the handler
	err := d.deregister()
	if err != nil {
		return fmt.Errorf("Error stopping MessageHandler: %s", err)
	}
	return nil
}
