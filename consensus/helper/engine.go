/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package helper

import (
	"github.com/hyperledger/fabric/consensus"
	"github.com/hyperledger/fabric/core/peer"

	"fmt"
	"sync"

	"github.com/hyperledger/fabric/consensus/controller"
	"github.com/hyperledger/fabric/consensus/util"
	"github.com/hyperledger/fabric/core/chaincode"
	pb "github.com/hyperledger/fabric/protos"
	"golang.org/x/net/context"
)

// EngineImpl implements a struct to hold consensus.Consenter, PeerEndpoint and MessageFan
type EngineImpl struct {
	consenter    consensus.Consenter
	helper       *Helper
	peerEndpoint *pb.PeerEndpoint
	consensusFan *util.MessageFan
	handlerMan	 *peer.HandlerManager
}

// GetHandlerFactory returns new NewConsensusHandler
func (eng *EngineImpl) GetHandlerFactory() peer.HandlerFactory {
	return NewConsensusHandler
}

// ProcessTransactionMsg processes a Message in context of a Transaction
func (eng *EngineImpl) ProcessTransactionMsg(msg *pb.Message, tx *pb.Transaction) (response *pb.Response) {
	//TODO: Do we always verify security, or can we supply a flag on the invoke ot this functions so to bypass check for locally generated transactions?
	if tx.Type == pb.Transaction_CHAINCODE_QUERY {
		if !engine.helper.valid {
			logger.Warning("Rejecting query because state is currently not valid")
			return &pb.Response{Status: pb.Response_FAILURE,
				Msg: []byte("Error: state may be inconsistent, cannot query")}
		}

		// The secHelper is set during creat ChaincodeSupport, so we don't need this step
		// cxt := context.WithValue(context.Background(), "security", secHelper)
		cxt := context.Background()
		//query will ignore events as these are not stored on ledger (and query can report
		//"event" data synchronously anyway)
		result, _, err := chaincode.Execute(cxt, chaincode.GetChain(chaincode.DefaultChain), tx)
		if err != nil {
			response = &pb.Response{Status: pb.Response_FAILURE,
				Msg: []byte(fmt.Sprintf("Error:%s", err))}
		} else {
			response = &pb.Response{Status: pb.Response_SUCCESS, Msg: result}
		}
	} else {
		// Chaincode Transaction
		response = &pb.Response{Status: pb.Response_SUCCESS, Msg: []byte(tx.Txid)}

		//TODO: Do we need to verify security, or can we supply a flag on the invoke ot this functions
		// If we fail to marshal or verify the tx, don't send it to consensus plugin
		if response.Status == pb.Response_FAILURE {
			return response
		}

		// Pass the message to the consenter (eg. PBFT) NOTE: Make sure engine has been initialized
		if eng.consenter == nil {
			return &pb.Response{Status: pb.Response_FAILURE, Msg: []byte("Engine not initialized")}
		}
		// TODO, do we want to put these requests into a queue? This will block until
		// the consenter gets around to handling the message, but it also provides some
		// natural feedback to the REST API to determine how long it takes to queue messages
		err := eng.consenter.RecvMsg(msg, eng.peerEndpoint.ID)
		if err != nil {
			response = &pb.Response{Status: pb.Response_FAILURE, Msg: []byte(err.Error())}
		}
	}
	return response
}

func (eng *EngineImpl) setConsenter(consenter consensus.Consenter) *EngineImpl {
	eng.consenter = consenter
	return eng
}

func (eng *EngineImpl) setPeerEndpoint(peerEndpoint *pb.PeerEndpoint) *EngineImpl {
	eng.peerEndpoint = peerEndpoint
	return eng
}

func (eng *EngineImpl) GetChannelWithConsensusServer() (*peer.HandlerManager,error) {
	if eng.handlerMan != nil{
		return eng.handlerMan,nil
	}
	return nil,fmt.Errorf("channel server is nil")
}

var engineOnce sync.Once

var engine *EngineImpl

func getEngineImpl() *EngineImpl {
	return engine
}

// GetEngine returns initialized peer.Engine
func GetEngine(coord peer.MessageHandlerCoordinator) (peer.Engine, error) {
	var err error
	engineOnce.Do(func() {
		engine = new(EngineImpl)

		ep, err := coord.GetPeerEndpoint()
		server,err := peer.NewChannelWithConsensus(ep)
		if err == nil{
			engine.handlerMan  = server
		}else{
			engine.handlerMan  = nil
			logger.Errorf("Error %s",err)
		}

		engine.helper = NewHelper(coord,engine.handlerMan)
		engine.consenter = controller.NewConsenter(engine.helper)
		engine.helper.setConsenter(engine.consenter)
		engine.peerEndpoint, err = coord.GetPeerEndpoint()
		engine.consensusFan = util.NewMessageFan()
		
		
		go func() {
			logger.Debug("Starting up message thread for recv tx msg")

			for{
				// The channel never closes, so this should never break
				select {
				case ocMsg := <-engine.consensusFan.GetOutChannel():
					//recv request msg
					engine.consenter.RecvMsg(ocMsg.Msg, ocMsg.Sender)
			
				case ocMsg2 := <-engine.handlerMan.GetOutChannel():
					//recv consensus msg
					engine.consenter.RecvMsg(ocMsg2.Msg, ocMsg2.Sender)				
				}
			}
		}()
	})
	return engine, err
}
