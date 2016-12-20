package peer
import (
    "fmt"
    "time"
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
	doneChan              chan struct{}  //new
	initiatedStream       bool // Was the stream initiated within this Peer
}


func NewChanHandler(coord MessageHandlerCoordinatorC, stream ChatStream,initiatedStream bool) (*ChanHandler,error) {
	var err error
	d := &ChanHandler{
		ChatStream:      stream,
		Coordinator:     coord,
		registered:  	 false,
		initiatedStream: initiatedStream,
	}
	d.consenterChan = make(chan *util.Message, 1000)
	d.doneChan = make(chan struct{})
	go func (){
		outChan := d.Coordinator.GetOutChannel()
		for {
			select {
			case msg := <- d.consenterChan:
				outChan <-msg

			case <-d.doneChan:
				sendPE,_ := d.To()
				selfPE := d.Coordinator.GetSelfPeerEndpoint()
				peerLogger.Errorf("Stopping recv Message from %v to %v",selfPE.ID,sendPE.ID)
				return 
			}
		}
	}()
	if initiatedStream {
		logger.Errorf("----send hello to ")
		if err = d.SendHello(); err != nil{
			return nil,err
		}
	}
	return d,err
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
		selfPoint := d.Coordinator.GetSelfPeerEndpoint()
		if selfPoint == nil{
			return fmt.Errorf("self Endpoint is nil")
		}
		helloMessage := &pb.HelloMessage{}
		err := proto.Unmarshal(msg.Payload, helloMessage)
		if err != nil {
			return fmt.Errorf("Error unmarshalling HelloMessage: %s", err)
		}

		d.ToPeerEndpoint = helloMessage.PeerEndpoint

		if d.registered == true{
			logger.Errorf("%v recv msg type %s from %v,registered:%t",selfPoint.ID,pb.Message_DISC_HELLO,d.ToPeerEndpoint.ID,d.registered)
			return nil
		}else{
			if err = d.SendHello();err != nil{
				return err
			}
		}

		err = d.Coordinator.RegisterHandler(d)
		if err != nil{
			logger.Errorf("Register handler id: %v registered:%t,errror %s", d.ToPeerEndpoint.ID,d.registered,err)
			return err
		}else{
			d.registered = true
		    logger.Warningf("Register handler id: %v registered:%t ok",d.ToPeerEndpoint.ID,d.registered)
		    go d.start()
		}
		return nil
	}
	return fmt.Errorf("Did not handle message of type %s, passing on to next MessageHandler", msg.Type)
}

func (d *ChanHandler) start() {
	sendPE,_ := d.To()
	selfPE := d.Coordinator.GetSelfPeerEndpoint()
	tickChan := time.NewTicker(time.Second * 10).C
	logger.Warningf("Starting send hello Message service")
	for {
	       <-tickChan
	      if err := d.SendHello(); err != nil {
	          peerLogger.Errorf("Error sending %s from %v to %v during tick: %s", pb.Message_DISC_HELLO,selfPE.ID,sendPE.ID, err)
              peerLogger.Errorf("Stopping send hello Message from %v to %v",selfPE.ID,sendPE.ID)
              return
	      }
	}
}

func (d *ChanHandler) SendMessage(msg *pb.Message) error {
	//make sure Sends are serialized. Also make sure everyone uses SendMessage
	//instead of calling Send directly on the grpc stream
	d.chatMutex.Lock()
	defer d.chatMutex.Unlock()
	if d.ToPeerEndpoint == nil{
		logger.Infof("Sending message to stream of type:%s", msg.Type)
	}else{
		logger.Infof("Sending message to stream of type:%s to:%v", msg.Type,d.ToPeerEndpoint.ID)
	}
	
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
		err = d.Coordinator.DeregisterHandler(d)
		d.registered = false
		d.doneChan <- struct{}{}
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

func (d *ChanHandler) SendHello() error{
	helloMessage := &pb.HelloMessage{PeerEndpoint: d.Coordinator.GetSelfPeerEndpoint()}
	data, err := proto.Marshal(helloMessage)
	if err != nil {
		return fmt.Errorf("Error marshalling HelloMessage: %s", err)
	}
	// Need to sign the Discovery Hello message
	newDiscoveryHelloMsg := &pb.Message{Type: pb.Message_DISC_HELLO, Payload: data, Timestamp: cutil.CreateUtcTimestamp()}
	//err = p.signMessageMutating(newDiscoveryHelloMsg)
	if err := d.SendMessage(newDiscoveryHelloMsg); err != nil {
		return fmt.Errorf("Error sending %s during SendHello: %s", pb.Message_DISC_HELLO, err)
	}
	return nil
}
