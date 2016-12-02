package peer
import (
	"fmt"
	"sync"
    "github.com/hyperledger/fabric/consensus/util"
    pb "github.com/hyperledger/fabric/protos"
     "github.com/op/go-logging"
)
var logger = logging.MustGetLogger("ChanHandler")

type MessageHandlerCoordinatorC interface {
	RegisterHandler(endpoint *pb.PeerEndpoint) error  
	DeregisterHandler(messageHandler MessageHandler) error
	Broadcast(*pb.Message, pb.PeerEndpoint_Type) []error
	Unicast(*pb.Message, *pb.PeerID) error
	GetOutChannel() chan *util.Message
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
	if point != nil{
		d.ToPeerEndpoint = point
		d.registered = true
		logger.Testf("----NewChanHandler  create ok toPoint:%s",point.Address)
	}
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
	logger.Testf("----HandleMessage----- ")
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
	}

	return fmt.Errorf("Did not handle message of type %s, passing on to next MessageHandler", msg.Type)
}

func (d *ChanHandler) SendMessage(msg *pb.Message) error {
	logger.Testf("----SendMessage----- ")
	//make sure Sends are serialized. Also make sure everyone uses SendMessage
	//instead of calling Send directly on the grpc stream
	d.chatMutex.Lock()
	defer d.chatMutex.Unlock()
	logger.Debugf("Sending message to stream of type: %s ", msg.Type)
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
