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

package spbft

import (
	"fmt"
	"google/protobuf"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/consensus"
	"github.com/hyperledger/fabric/consensus/util/events"
	pb "github.com/hyperledger/fabric/protos"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type obcBatch struct {
	obcGeneric
	externalEventReceiver
	pbft        *spbftCore
	broadcaster *broadcaster

	batchSize  int
	batchStore []*Request
	//batchTimer       events.Timer
	batchTimerActive bool
	batchTimeout     time.Duration

	manager events.Manager // TODO, remove eventually, the event manager

	bTimer       *time.Timer       //new
	incomingChan chan batchMessage // Queues messages for processing by main thread

	deduplicatorChan chan lastExecTime //new

	idleChan chan struct{} // Idle channel, to be removed

	reqStore *requestStore // Holds the outstanding and pending requests

	deduplicator *deduplicator

	persistForward
}

type lossTxOperation struct {
	operation string
	txid      string
}

type batchMessage struct {
	msg    *pb.Message
	sender *pb.PeerID
}

type batch struct { //new
	requests []*Request
}

type lastExecTime struct { //new
	id           uint64
	lastExecTime time.Time
}

// Event types

// batchMessageEvent is sent when a consensus message is received that is then to be sent to pbft
type batchMessageEvent batchMessage

type batchEvent batch //new

// batchTimerEvent is sent when the batch timer expires
type batchTimerEvent struct{}

func newObcBatch(id uint64, config *viper.Viper, stack consensus.Stack) *obcBatch {
	var err error

	op := &obcBatch{
		obcGeneric: obcGeneric{stack: stack},
	}

	op.persistForward.persistor = stack

	logger.Debugf("Replica %d obtaining startup information", id)

	op.manager = events.NewManagerImpl() // TODO, this is hacky, eventually rip it out
	op.manager.SetReceiver(op)
	etf := events.NewTimerFactoryImpl(op.manager)
	op.pbft = newSPbftCore(id, config, op, etf)
	op.manager.Start()
	op.externalEventReceiver.manager = op.manager
	op.broadcaster = newBroadcaster(id, op.pbft.N, op.pbft.f, op.pbft.broadcastTimeout, stack)

	op.batchSize = config.GetInt("general.batchsize")
	op.batchStore = nil
	op.batchTimeout, err = time.ParseDuration(config.GetString("general.timeout.batch"))
	if err != nil {
		panic(fmt.Errorf("Cannot parse batch timeout: %s", err))
	}
	logger.Infof("SPBFT Batch size = %d", op.batchSize)
	logger.Infof("SPBFT Batch timeout = %v", op.batchTimeout)

	if op.batchTimeout >= op.pbft.requestTimeout {
		op.pbft.requestTimeout = 3 * op.batchTimeout / 2
		logger.Warningf("Configured request timeout must be greater than batch timeout, setting to %v", op.pbft.requestTimeout)
	}

	if op.pbft.requestTimeout >= op.pbft.nullRequestTimeout && op.pbft.nullRequestTimeout != 0 {
		op.pbft.nullRequestTimeout = 3 * op.pbft.requestTimeout / 2
		logger.Warningf("Configured null request timeout must be greater than request timeout, setting to %v", op.pbft.nullRequestTimeout)
	}

	op.incomingChan = make(chan batchMessage, op.batchSize) //new

	op.deduplicatorChan = make(chan lastExecTime, op.batchSize) //new


	//op.batchTimer = etf.CreateTimer()

	op.reqStore = newRequestStore()

	op.deduplicator = newDeduplicator()

	op.idleChan = make(chan struct{})
	close(op.idleChan) // TODO remove eventually
	//new
	op.bTimer = time.NewTimer(op.batchTimeout) // start timer now so we can just reset it
	op.bTimer.Stop()
	go op.handleChannels()

	return op
}

// Close tells us to release resources we are holding
func (op *obcBatch) Close() {
	//op.batchTimer.Halt()
	op.pbft.close()
}


// func (op *obcBatch) broadcastRequestMsg(msg *BatchMessage) {
// 	msgPayload, _ := proto.Marshal(msg)

// 	ocMsg := &pb.Message{
// 		Type:    pb.Message_REQUEST,
// 		Payload: msgPayload,
// 	}
// 	op.broadcaster.Broadcast(ocMsg)
// }

// // send a message to a specific replica
// func (op *obcBatch) unicastRequestMsg(msg *BatchMessage, receiverID uint64) {
// 	msgPayload, _ := proto.Marshal(msg)
// 	ocMsg := &pb.Message{
// 		Type:    pb.Message_REQUEST,
// 		Payload: msgPayload,
// 	}
// 	op.broadcaster.Unicast(ocMsg, receiverID)
// }

func (op *obcBatch) broadcastMsg(msg *BatchMessage) {
	msgPayload, _ := proto.Marshal(msg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: msgPayload,
	}
	op.broadcaster.Broadcast(ocMsg)
}

// send a message to a specific replica
func (op *obcBatch) unicastMsg(msg *BatchMessage, receiverID uint64) {
	msgPayload, _ := proto.Marshal(msg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: msgPayload,
	}
	op.broadcaster.Unicast(ocMsg, receiverID)
}

// =============================================================================
// helper functions for clean Tx
// =============================================================================

func (op *obcBatch) cleanTx(highBlock uint64, lowBlock uint64) {
	for cleanBlock := lowBlock; cleanBlock <= highBlock; cleanBlock++ {
		block, err := op.stack.GetBlock(cleanBlock)
		if err == nil {
			blockTransactions := block.GetTransactions()
			for _, transaction := range blockTransactions {
				if outstanding, pending := op.reqStore.removeTx(transaction.Txid); !outstanding || !pending {
					logger.Infof("node %d missing transaction %s outstanding=%v, pending=%v", op.pbft.id, transaction.Txid, outstanding, pending)
				} else {
					logger.Infof("node %d success delete transaction %s", op.pbft.id, transaction.Txid)
				}
			}
		}
	}
}

// =============================================================================
// innerStack interface (functions called by pbft-core)
// =============================================================================

func (op *obcBatch) getcurrentBlockNumber() uint64 {

	return op.stack.GetBlockchainSize() - 1
}

// multicast a message to all replicas

func (op *obcBatch) broadcast(msgPayload []byte) {
	op.broadcaster.Broadcast(op.wrapMessage(msgPayload))
}

// send a message to a specific replica

func (op *obcBatch) unicast(msgPayload []byte, receiverID uint64) (err error) {
	return op.broadcaster.Unicast(op.wrapMessage(msgPayload), receiverID)
}

func (op *obcBatch) sign(msg []byte) ([]byte, error) {
	return op.stack.Sign(msg)
}

// verify message signature
func (op *obcBatch) verify(senderID uint64, signature []byte, message []byte) error {
	senderHandle, err := getValidatorHandle(senderID)
	if err != nil {
		return err
	}
	return op.stack.Verify(senderHandle, signature, message)
}



func (op *obcBatch) execute(seqNo uint64, reqBatch *RequestBatch) {
	var txs []*pb.Transaction
	for _, req := range reqBatch.GetBatch() {
		tx := &pb.Transaction{}
		if err := proto.Unmarshal(req.Payload, tx); err != nil {
			logger.Warningf("Batch replica %d could not unmarshal transaction %s", op.pbft.id, err)
			continue
		}
		logger.Debugf("Batch replica %d executing request with transaction %s from outstandingReqs, seqNo=%d", op.pbft.id, tx.Txid, seqNo)
		logger.Infof("---node %d begin to delete tx %s", op.pbft.id, tx.Txid)
		//logger.Infof("---node %d begin to delete tx %s", op.pbft.id, tx.Txid)
		outstanding, pending := op.reqStore.remove(req) 
		if !outstanding || !pending {
			logger.Infof("Batch replica %d missing transaction %s outstanding=%v, pending=%v", op.pbft.id, tx.Txid, outstanding, pending)
		}
		txs = append(txs, tx)
		op.deduplicator.Execute(req)
	}
	meta, _ := proto.Marshal(&Metadata{seqNo})
	logger.Debugf("Batch replica %d received exec for seqNo %d containing %d transactions", op.pbft.id, seqNo, len(txs))
	op.stack.Execute(meta, txs) // This executes in the background, we will receive an executedEvent once it completes
}

// =============================================================================
// functions specific to batch mode
// =============================================================================

func (op *obcBatch) txToReq(tx []byte) *Request {
	now := time.Now()
	transaction := &pb.Transaction{}
	if err := proto.Unmarshal(tx, transaction); err != nil {
		logger.Warningf("Batch replica %d could not unmarshal transaction %s", op.pbft.id, err)
		return nil
	}
	req := &Request{
		Timestamp: &google_protobuf.Timestamp{
			Seconds: now.Unix(),
			Nanos:   int32(now.UnixNano() % 1000000000),
		},
		Payload:   tx,
		ReplicaId: op.pbft.id,
		Txid:      transaction.Txid,
	}
	// XXX sign req
	return req
}


func (op *obcBatch) logAddTxFromRequest(req *Request) {
	if logger.IsEnabledFor(logging.INFO) {
		// This is potentially a very large expensive debug statement, guard
		tx := &pb.Transaction{}
		err := proto.Unmarshal(req.Payload, tx)
		if err != nil {
			logger.Errorf("Replica %d was sent a transaction which did not unmarshal: %s", op.pbft.id, err)
		} else {
			logger.Debugf("Replica %d adding request from %d with transaction %s into outstandingReqs", op.pbft.id, req.ReplicaId, tx.Txid)
			logger.Infof("Replica %d adding request from %d with transaction %s", op.pbft.id, req.ReplicaId, tx.Txid)
		}
	}
}

func (op *obcBatch) resubmitOutstandingReqs() events.Event {
	op.startTimerIfOutstandingRequests()
	logger.Infof("----resubmitOutsta--id:%d begin---", op.pbft.id)

	// If we are the primary, and know of outstanding requests, submit them for inclusion in the next batch until
	// we run out of requests, or a new batch message is triggered (this path will re-enter after execution)
	// Do not enter while an execution is in progress to prevent duplicating a request
	if op.pbft.primary(op.pbft.view) == op.pbft.id && op.pbft.IsSendBatch() && op.pbft.currentExec == nil {
		//needed := op.batchSize- len(op.batchStore)
		needed := op.batchSize //

		for op.reqStore.hasNonPending() {
			outstanding := op.reqStore.getNextNonPending(needed)
			op.reqStore.storePendings(outstanding)
			requestBatch := &RequestBatch{Batch: outstanding}
			logger.Infof("----resubmitOutsta--id:%d size:%d", op.pbft.id, len(outstanding))
			op.manager.Inject(requestBatch)
		}
	}
	return nil
}

func (op *obcBatch) sendBatchNew(reason string) events.Event {
	op.bTimer.Stop()
	op.batchTimerActive = false
	if len(op.batchStore) == 0 {
		logger.Error("Told to send an empty batch store for ordering, ignoring")
		return nil
	}

	reqBatch := &batchEvent{requests: op.batchStore}
	op.batchStore = nil
	logger.Infof("Replica %d Creating batch %s with %d requests", op.pbft.id, reason, len(reqBatch.requests))
	return reqBatch
}

func (op *obcBatch) RecvMsg(ocMsg *pb.Message, senderHandle *pb.PeerID) error {
	op.distributeMsg(ocMsg, senderHandle)
	return nil
}

func (op *obcBatch) distributeMsg(ocMsg *pb.Message, senderHandle *pb.PeerID) {
	if ocMsg.Type == pb.Message_CHAIN_TRANSACTION  {
		op.incomingChan <- batchMessage{ //tx
			msg:    ocMsg,
			sender: senderHandle,
		}
		return
	} else {
		if ocMsg.Type != pb.Message_CONSENSUS {
			logger.Errorf("Unexpected message type: %s", ocMsg.Type)
			return
		}

		batchMsg := &BatchMessage{}
		err := proto.Unmarshal(ocMsg.Payload, batchMsg)
		if err != nil {
			logger.Errorf("Error unmarshaling message: %s", err)
			return
		}

		if req := batchMsg.GetRequest(); req != nil {
			op.incomingChan <- batchMessage{ //req
				msg:    ocMsg,
				sender: senderHandle,
			}
			return
		} else if pbftMsg := batchMsg.GetPbftMessage(); pbftMsg != nil {
			senderID, err := getValidatorID(senderHandle) // who sent this?
			if err != err {
				panic("Cannot map sender's PeerID to a valid replica ID")
			}
			msg := &Message{}
			err = proto.Unmarshal(pbftMsg, msg)
			if err != nil {
				logger.Errorf("Error unpacking payload from message: %s", err)
				return
			}
			op.externalEventReceiver.manager.Queue() <- pbftMessageEvent{ //pbft msg
				msg:    msg,
				sender: senderID,
			}
			return
		}
		logger.Errorf("Unknown request: %+v", batchMsg)
	}
	return
}

func (op *obcBatch) handleChannels() {
	var event events.Event
	for {
		select {
		case ocMsg := <-op.incomingChan:
			event = op.processMessageNew(ocMsg.msg, ocMsg.sender)
			if event != nil {
				op.externalEventReceiver.manager.Queue() <- event
			}
		case <-op.bTimer.C:
			event = op.sendBatchNew("timer expired")
			if event != nil {
				op.externalEventReceiver.manager.Queue() <- event
			}
		}
	}
}


func (op *obcBatch) processMessageNew(ocMsg *pb.Message, senderHandle *pb.PeerID) events.Event {
	submitBatch := func(req *Request) events.Event {
		op.logAddTxFromRequest(req)
		op.batchStore = append(op.batchStore, req)
		if !op.batchTimerActive {
			op.bTimer.Reset(op.batchTimeout)
			op.batchTimerActive = true
		}
		if len(op.batchStore) >= op.batchSize {
			return op.sendBatchNew("")
		}
		return nil
	}

	// if ocMsg.Type == pb.Message_CHAIN_TRANSACTION {
	// 	req := op.txToReq(ocMsg.Payload)
	// 	go op.broadcastRequestMsg(&BatchMessage{Payload: &BatchMessage_Request{Request: req}})
	// 	return submitBatch(req)
	// }
	if ocMsg.Type == pb.Message_CHAIN_TRANSACTION {
		req := op.txToReq(ocMsg.Payload)
		op.broadcastMsg(&BatchMessage{Payload: &BatchMessage_Request{Request: req}})
		return submitBatch(req)
	}

	if ocMsg.Type != pb.Message_CONSENSUS {
		logger.Errorf("Unexpected message type: %s", ocMsg.Type)
		return nil
	}

	batchMsg := &BatchMessage{}
	err := proto.Unmarshal(ocMsg.Payload, batchMsg)
	if err != nil {
		logger.Errorf("Error unmarshaling message: %s", err)
		return nil
	}

	if req := batchMsg.GetRequest(); req != nil {
		return submitBatch(req)
	} else {
		logger.Errorf("Unexpected message type: %s", ocMsg.Type)
	}
	return nil
}

// allow the primary to send a batch when the timer expires
func (op *obcBatch) ProcessEvent(event events.Event) events.Event {
	logger.Debugf("Replica %d batch main thread looping", op.pbft.id)
	switch et := event.(type) {
	case *batchEvent: //new
		var req []*Request
		for _, tx := range et.requests{
			if op.deduplicator.IsNew(tx) {
				logger.Infof("node %d add New tx %s the primary is %d", op.pbft.id, tx.Txid, op.pbft.primary(op.pbft.view))
				req = append(req, tx)
			}else{
				logger.Infof("node %d add New tx %s for tx is not new, the primary is %d", op.pbft.id, tx.Txid, op.pbft.primary(op.pbft.view))
			}
		}  
		op.reqStore.storeOutstandings(req)
		op.startTimerIfOutstandingRequests()
		if (op.pbft.primary(op.pbft.view) == op.pbft.id) && op.pbft.IsSendBatch() && op.pbft.primarySendBatch {
			// op.reqStore.storePendings(req)
			// reqBatch := &RequestBatch{Batch: req}
			// return reqBatch
			needed := op.batchSize
			if op.reqStore.outstandingRequests.Len() < needed {
				needed = op.reqStore.outstandingRequests.Len()
			}
			outstanding := op.reqStore.getNextNonPending(needed)
			op.reqStore.storePendings(outstanding)
			for _, tx := range outstanding{
				logger.Infof("003node %d add Tx %s to batch", op.pbft.id, tx.Txid)
			}
			requestBatch := &RequestBatch{Batch: outstanding}
			return requestBatch
		}

	case executedEvent:
		op.stack.Commit(nil, et.tag.([]byte))
	case committedEvent:
		logger.Debugf("Replica %d received committedEvent", op.pbft.id)
		return execDoneEvent{}
	case execDoneEvent:
		if res := op.pbft.ProcessEvent(event); res != nil {
			// This may trigger a view change, if so, process it, we will resubmit on new view
			return res
		}
		logger.Infof("---batch---id:%d outstandReq:%d  pendReq:%d", op.pbft.id, op.reqStore.outstandingRequests.Len(), op.reqStore.pendingRequests.Len())
		return op.resubmitOutstandingReqs()
	case *Commit:
		// TODO, this is extremely hacky, but should go away when batch and core are merged
		res := op.pbft.ProcessEvent(event)
		op.startTimerIfOutstandingRequests()
		return res
	case viewChangedEvent:
		//op.batchStore = nil
		// Outstanding reqs doesn't make sense for batch, as all the requests in a batch may be processed
		// in a different batch, but PBFT core can't see through the opaque structure to see this
		// so, on view change, clear it out
		op.pbft.outstandingReqBatches = make(map[string]*RequestBatch)

		logger.Debugf("Replica %d batch thread recognizing new view", op.pbft.id)
		/*if op.batchTimerActive {
			op.stopBatchTimer()
		}*/

		if op.pbft.skipInProgress {
			// If we're the new primary, but we're in state transfer, we can't trust ourself not to duplicate things
			logger.Infof("---skipInProgress---node %d skipInProgress begin to clear outstandingRequests, the len is %d", op.pbft.id, op.reqStore.outstandingRequests.Len())
			for oreqc := op.reqStore.outstandingRequests.order.Front(); oreqc != nil; oreqc = oreqc.Next() {
				oreq := oreqc.Value.(requestContainer)
				logger.Infof("001node %d begin to delete %s", op.pbft.id, oreq.key)
				//oreq.key
			}
			op.reqStore.outstandingRequests.empty()
			logger.Infof("node %d is skipInProgress but we not empty Requests, current view:%d block%d", op.pbft.id, op.pbft.view, op.stack.GetBlockchainSize()-1)
		}

		op.reqStore.pendingRequests.empty()
		for i := op.pbft.h + 1; i <= op.pbft.h+op.pbft.L; i++ {
			if i <= op.pbft.lastExec {
				continue
			}

			cert, ok := op.pbft.certStore[msgID{v: op.pbft.view, n: i}]
			if !ok || cert.prePrepare == nil {
				continue
			}

			if cert.prePrepare.BatchDigest == "" {
				// a null request
				continue
			}

			if cert.prePrepare.RequestBatch == nil {
				logger.Warningf("Replica %d found a non-null prePrepare with no request batch, ignoring")
				continue
			}

			op.reqStore.storePendings(cert.prePrepare.RequestBatch.GetBatch())
		}
		op.pbft.primarySendBatch = true
		logger.Infof("node %d start primarySendBatch", op.pbft.id)
		return op.resubmitOutstandingReqs()
	case stateUpdatedEvent:
		// When the state is updated, clear any outstanding requests, they may have been processed while we were gone
		logger.Infof("---stateUpdatedEvent---node %d stateUpdatedEvent begin to clear outstandingRequests, the len is %d", op.pbft.id, op.reqStore.outstandingRequests.Len())
		for oreqc := op.reqStore.outstandingRequests.order.Front(); oreqc != nil; oreqc = oreqc.Next() {
			oreq := oreqc.Value.(requestContainer)
			logger.Infof("002node %d begin to delete %s", op.pbft.id, oreq.key)
			//oreq.key
		}
		op.reqStore = newRequestStore()
		// logger.Infof("-----targer is not nil--------")
		// //targerNumber := et.target.Height - 1
		// info := &pb.BlockchainInfo{}
		// err := proto.Unmarshal(op.pbft.highStateTarget.id, info) 
		// if err != nil {
		// 	logger.Error(fmt.Sprintf("Error unmarshaling: %s", err))
		// 	return
		// }  
		// targerNumber := info.Height - 1
		// logger.Infof("targerNumber is %d", targerNumber)
		// logger.Infof("node %d begin to clean Tx from block:%d to block:%d", op.pbft.id, op.pbft.badBlockNumber, targerNumber)
		// op.cleanTx(targerNumber, op.pbft.badBlockNumber)
		// logger.Infof("-----------")
		return op.pbft.ProcessEvent(event)
	default:
		return op.pbft.ProcessEvent(event)
	}

	return nil
}


// Wraps a payload into a batch message, packs it and wraps it into
// a Fabric message. Called by broadcast before transmission.
func (op *obcBatch) wrapMessage(msgPayload []byte) *pb.Message {
	batchMsg := &BatchMessage{Payload: &BatchMessage_PbftMessage{PbftMessage: msgPayload}}
	packedBatchMsg, _ := proto.Marshal(batchMsg)
	ocMsg := &pb.Message{
		Type:    pb.Message_CONSENSUS,
		Payload: packedBatchMsg,
	}
	return ocMsg
}

// Retrieve the idle channel, only used for testing
func (op *obcBatch) idleChannel() <-chan struct{} {
	return op.idleChan
}

// TODO, temporary
func (op *obcBatch) getManager() events.Manager {
	return op.manager
}

func (op *obcBatch) startTimerIfOutstandingRequests() {
	if op.pbft.skipInProgress || op.pbft.currentExec != nil || !op.pbft.activeView {
		// Do not start view change timer if some background event is in progress
		logger.Debugf("Replica %d not starting timer because skip in progress or current exec or in view change", op.pbft.id)
		return
	}

	if !op.reqStore.hasNonPending() {
		// Only start a timer if we are aware of outstanding requests
		logger.Debugf("Replica %d not starting timer because all outstanding requests are pending", op.pbft.id)
		return
	}
	op.pbft.softStartTimer(op.pbft.requestTimeout, "Batch outstanding requests")
}
