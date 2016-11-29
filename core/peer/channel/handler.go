package channel
import (
	"fmt"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/consensus/helper"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/consensus/util"

	pb "github.com/hyperledger/fabric/protos"
)

//实现peer.go中MessageHandler接口
/*
type MessageHandler interface {  
	RemoteLedger
	HandleMessage(msg *pb.Message) error //处理接受到的消息
	SendMessage(msg *pb.Message) error  //发送消息
	To() (pb.PeerEndpoint, error)
	Stop() error
}

type ConsensusHandler struct {
	peer.MessageHandler
	consenterChan chan *util.Message
	coordinator   peer.MessageHandlerCoordinator
}*/

type Handler struct {
	peer.MessageHandler
	chatMutex             sync.Mutex
	Coordinator           MessageHandlerCoordinator
	ChatStream            ChatStream 
	consenterChan 		  chan *util.Message
}


func NewHandler(coord MessageHandlerCoordinator, stream ChatStream) (MessageHandler, error) {
	d := &Handler{
		ChatStream:      stream,
		Coordinator:     coord,
		consenterChan    chan *util.Message
	}
	d.consenterChan = make(chan *util.Message, 1000)
	go func (){
		outChan := d.coordinator.GetOutChannel()
		for msg := range d.consenterChan { 
			outChan <-msg
		}
	}
	return d, nil
}

// HandleMessage handles the incoming Fabric messages for the Peer
func (d *Handler) HandleMessage(msg *pb.Message) error { //接受节点消息
	if msg.Type == pb.Message_CONSENSUS { //共识消息
		senderPE, _ := handler.To()
		select {
		case handler.consenterChan <- &util.Message{
				Msg:    msg,
				Sender: senderPE.ID,
			}:
			return nil
		default:
			err := fmt.Errorf("Message channel for %v full, rejecting", senderPE.ID)
			logger.Errorf("Failed to queue consensus message because: %v", err)
			return err
		}
	}

	return fmt.Errorf("Did not handle message of type %s, passing on to next MessageHandler", msg.Type)
}

func (d *Handler) SendMessage(msg *pb.Message) error {
	//make sure Sends are serialized. Also make sure everyone uses SendMessage
	//instead of calling Send directly on the grpc stream
	d.chatMutex.Lock()
	defer d.chatMutex.Unlock()
	peerLogger.Debugf("Sending message to stream of type: %s ", msg.Type)
	err := d.ChatStream.Send(msg)
	if err != nil {
		return fmt.Errorf("Error Sending message through ChatStream: %s", err)
	}
	return nil
}

func (d *Handler) To() (pb.PeerEndpoint, error) {
	if d.ToPeerEndpoint == nil {
		return pb.PeerEndpoint{}, fmt.Errorf("No peer endpoint for handler")
	}
	return *(d.ToPeerEndpoint), nil
}

func (d *Handler) Stop() error{
	// Deregister the handler
	err := d.Coordinator.DeregisterHandler(d)
	if err != nil {
		return fmt.Errorf("Error stopping MessageHandler: %s", err)
	}
	return nil
}
