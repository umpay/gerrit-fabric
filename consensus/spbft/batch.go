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

	"github.com/hyperledger/fabric/consensus"
	"github.com/hyperledger/fabric/consensus/util/events"
	pb "github.com/hyperledger/fabric/protos"

	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type obcBatch struct {
	obcGeneric
	externalEventReceiver
	pbft        *spbftCore
	broadcaster *broadcaster

	batchSize        int
	batchStore       []*Request
	//batchTimer       events.Timer
	batchTimerActive bool
	batchTimeout     time.Duration

	manager events.Manager // TODO, remove eventually, the event manager

	bTimer    *time.Timer   //create batch timer
	incomingChan chan batchMessage // Queues messages for processing by main thread

	deduplicatorChan chan lastExecTime  //update lastExecTime chan

	idleChan     chan struct{}      // Idle channel, to be removed

	reqStore *requestStore // Holds the outstanding and pending requests

	deduplicator *deduplicator

	persistForward
}

type batchMessage struct {
	msg    *pb.Message
	sender *pb.PeerID
}

type batch struct {  
	requests  []*Request
}

type lastExecTime struct {
	id 			 uint64
	lastExecTime time.Time
}

// Event types

// batchMessageEvent is sent when a consensus message is received that is then to be sent to pbft
type batchMessageEvent batchMessage

//create new batch
type batchEvent batch  


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

	op.incomingChan = make(chan batchMessage,op.batchSize)  //chan cache is a block size

	op.deduplicatorChan = make(chan lastExecTime,10) //chan cache is 10
	
	//op.batchTimer = etf.CreateTimer()

	op.reqStore = newRequestStore()

	op.deduplicator = newDeduplicator()

	op.idleChan = make(chan struct{})
	close(op.idleChan) // TODO remove eventually

	op.bTimer = time.NewTimer(op.batchTimeout) // start timer now so we can just reset it
	op.bTimer.Stop()
	//Create a thread to accept the transaction
	go op.handleChannels() 

	return op
}

// Close tells us to release resources we are holding
func (op *obcBatch) Close() {
	//op.batchTimer.Halt()
	op.pbft.close()
}
/*
func (op *obcBatch) submitToLeader(req *Request) events.Event {
	// Broadcast the request to the network, in case we're in the wrong view
	op.broadcastMsg(&BatchMessage{Payload: &BatchMessage_Request{Request: req}})
	op.logAddTxFromRequest(req)
	op.reqStore.storeOutstanding(req)
	op.startTimerIfOutstandingRequests()
	if op.pbft.primary(op.pbft.view) == op.pbft.id && op.pbft.activeView {
		return op.leaderProcReq(req)
	}
	return nil
}
*/
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
// innerStack interface (functions called by pbft-core)
// =============================================================================

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

// execute an opaque request which corresponds to an OBC Transaction
func (op *obcBatch) execute(seqNo uint64, reqBatch *RequestBatch) {
	var txs []*pb.Transaction
	var execTime lastExecTime
	for _, req := range reqBatch.GetBatch() {
		tx := &pb.Transaction{}
		if err := proto.Unmarshal(req.Payload, tx); err != nil {
			logger.Warningf("Batch replica %d could not unmarshal transaction %s", op.pbft.id, err)
			continue
		}
		logger.Debugf("Batch replica %d executing request with transaction %s from outstandingReqs, seqNo=%d", op.pbft.id, tx.Txid, seqNo)
		if outstanding, pending := op.reqStore.remove(req); !outstanding || !pending {
			logger.Debugf("Batch replica %d missing transaction %s outstanding=%v, pending=%v", op.pbft.id, tx.Txid, outstanding, pending)
		}
		txs = append(txs, tx)

		reqTime := time.Unix(req.Timestamp.Seconds, int64(req.Timestamp.Nanos))
		if reqTime.After(execTime.lastExecTime) {
			execTime.lastExecTime = reqTime //Update request time
			execTime.id = req.ReplicaId
		}
	}
	op.deduplicatorChan <- execTime

	meta, _ := proto.Marshal(&Metadata{seqNo})
	logger.Debugf("Batch replica %d received exec for seqNo %d containing %d transactions", op.pbft.id, seqNo, len(txs))
	op.stack.Execute(meta, txs) // This executes in the background, we will receive an executedEvent once it completes
}

// =============================================================================
// functions specific to batch mode
// =============================================================================
/*
func (op *obcBatch) leaderProcReq(req *Request) events.Event {
	// XXX check req sig
	digest := hash(req)
	logger.Debugf("Batch primary %d queueing new request %s", op.pbft.id, digest)
	op.batchStore = append(op.batchStore, req)
	op.reqStore.storePending(req)

	if !op.batchTimerActive {
		op.startBatchTimer()
	}

	if len(op.batchStore) >= op.batchSize {
		return op.sendBatch()
	}

	return nil
}

func (op *obcBatch) sendBatch() events.Event {
	op.stopBatchTimer()
	if len(op.batchStore) == 0 {
		logger.Error("Told to send an empty batch store for ordering, ignoring")
		return nil
	}

	reqBatch := &RequestBatch{Batch: op.batchStore}
	op.batchStore = nil
	logger.Infof("Creating batch with %d requests", len(reqBatch.Batch))
	return reqBatch
}
*/
func (op *obcBatch) txToReq(tx []byte) *Request {
	now := time.Now()
	req := &Request{
		Timestamp: &google_protobuf.Timestamp{
			Seconds: now.Unix(),
			Nanos:   int32(now.UnixNano() % 1000000000),
		},
		Payload:   tx,
		ReplicaId: op.pbft.id,
	}
	// XXX sign req
	return req
}
/*
func (op *obcBatch) processMessage(ocMsg *pb.Message, senderHandle *pb.PeerID) events.Event {
	if ocMsg.Type == pb.Message_CHAIN_TRANSACTION {
		req := op.txToReq(ocMsg.Payload)
		return op.submitToLeader(req)
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
		if !op.deduplicator.IsNew(req) {
			logger.Warningf("Replica %d ignoring request as it is too old", op.pbft.id)
			return nil
		}

		op.logAddTxFromRequest(req)
		op.reqStore.storeOutstanding(req)
		if (op.pbft.primary(op.pbft.view) == op.pbft.id) && op.pbft.activeView {
			return op.leaderProcReq(req)
		}
		op.startTimerIfOutstandingRequests()
		return nil
	} else if pbftMsg := batchMsg.GetPbftMessage(); pbftMsg != nil {
		senderID, err := getValidatorID(senderHandle) // who sent this?
		if err != nil {
			panic("Cannot map sender's PeerID to a valid replica ID")
		}
		msg := &Message{}
		err = proto.Unmarshal(pbftMsg, msg)
		if err != nil {
			logger.Errorf("Error unpacking payload from message: %s", err)
			return nil
		}
		return pbftMessageEvent{
			msg:    msg,
			sender: senderID,
		}
	}

	logger.Errorf("Unknown request: %+v", batchMsg)

	return nil
}
*/
func (op *obcBatch) logAddTxFromRequest(req *Request) {
	if logger.IsEnabledFor(logging.DEBUG) {
		// This is potentially a very large expensive debug statement, guard
		tx := &pb.Transaction{}
		err := proto.Unmarshal(req.Payload, tx)
		if err != nil {
			logger.Errorf("Replica %d was sent a transaction which did not unmarshal: %s", op.pbft.id, err)
		} else {
			logger.Debugf("Replica %d adding request from %d with transaction %s into outstandingReqs", op.pbft.id, req.ReplicaId, tx.Txid)
		}
	}
}

func (op *obcBatch) resubmitOutstandingReqs() events.Event {
	op.startTimerIfOutstandingRequests()

	// If we are the primary, and know of outstanding requests, submit them for inclusion in the next batch until
	// we run out of requests, or a new batch message is triggered (this path will re-enter after execution)
	// Do not enter while an execution is in progress to prevent duplicating a request
	if op.pbft.primary(op.pbft.view) == op.pbft.id && op.pbft.IsSendBatch() && op.pbft.currentExec == nil {
		//needed := op.batchSize- len(op.batchStore)
		needed := op.batchSize //

		for op.reqStore.hasNonPending() {
			outstanding := op.reqStore.getNextNonPending(needed)
			op.reqStore.storePendings(outstanding)
			requestBatch :=&RequestBatch{Batch: outstanding}
			logger.Infof("Replica %d resubmit batch size:%d",op.pbft.id,len(outstanding))
			op.manager.Inject(requestBatch)
			/*
			// If we have enough outstanding requests, this will trigger a batch
			for _, nreq := range outstanding {
				if msg := op.leaderProcReq(nreq); msg != nil {
					op.manager.Inject(msg) //执行下一个批次
				}
			}*/
		}
	}
	return nil
}

func (op *obcBatch) sendBatchNew(reason string) events.Event {
	op.bTimer.Stop()
	op.batchTimerActive = false
	if len(op.batchStore) == 0 {
		logger.Errorf("Replica %d Told to send an empty batch store for ordering, ignoring",op.pbft.id)
		return nil
	}

	reqBatch := &batchEvent{requests: op.batchStore}
	op.batchStore = nil
	logger.Infof("Replica %d Creating batch %s with %d requests",op.pbft.id,reason,len(reqBatch.requests))
	return reqBatch
}

func (op *obcBatch) RecvMsg(ocMsg *pb.Message, senderHandle *pb.PeerID) error {
	op.distributeMsg(ocMsg,senderHandle)
	return nil
}

func (op *obcBatch) distributeMsg(ocMsg *pb.Message, senderHandle *pb.PeerID) {
	if ocMsg.Type == pb.Message_CHAIN_TRANSACTION {
		op.incomingChan <- batchMessage{  //tx
			msg:    ocMsg,
			sender: senderHandle,
		}
		return
	}else{
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
			op.incomingChan <- batchMessage{  //req
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
	for{
		select {
		case ocMsg := <-op.incomingChan:
			event = op.processMessageNew(ocMsg.msg, ocMsg.sender)
			if event != nil{
				op.externalEventReceiver.manager.Queue() <-event 
			}
		case <-op.bTimer.C:
			 event = op.sendBatchNew("timer expired")
			 if event != nil{
				op.externalEventReceiver.manager.Queue() <-event 
			}
		case  dp := <-op.deduplicatorChan:
			op.deduplicator.UpdateExecTime(dp.id,dp.lastExecTime)
		}
	}
}

func (op *obcBatch) processMessageNew(ocMsg *pb.Message, senderHandle *pb.PeerID) events.Event {
	submitBatch := func(req *Request) events.Event{
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
		if !op.deduplicator.IsNew(req) {
			logger.Warningf("Replica %d ignoring request as it is too old", op.pbft.id)
			return nil
		}
		return submitBatch(req)
	} else{
		logger.Errorf("Unexpected message type: %s", ocMsg.Type)
	}
	return nil
}

// allow the primary to send a batch when the timer expires
func (op *obcBatch) ProcessEvent(event events.Event) events.Event {
	logger.Debugf("Replica %d batch main thread looping", op.pbft.id)
	switch et := event.(type) {
	/*
	case batchMessageEvent:
		ocMsg := et
		return op.processMessage(ocMsg.msg, ocMsg.sender)
	*/
	case *batchEvent:  //new
		req := et.requests
		op.reqStore.storeOutstandings(req)
		op.startTimerIfOutstandingRequests()
		if (op.pbft.primary(op.pbft.view) == op.pbft.id) && op.pbft.IsSendBatch() {
			op.reqStore.storePendings(req)
			reqBatch := &RequestBatch{Batch: req}
			return reqBatch
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
		return op.resubmitOutstandingReqs()
	/*case batchTimerEvent:
		count[4] += 1
		logger.Infof("Replica %d batch timer expired", op.pbft.id)
		if op.pbft.activeView && (len(op.batchStore) > 0) {
			return op.sendBatch()
		}*/
	case *Commit:
		// TODO, this is extremely hacky, but should go away when batch and core are merged
		res := op.pbft.ProcessEvent(event)
		op.startTimerIfOutstandingRequests()
		return res
	case viewChangedEvent:
		op.batchStore = nil
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
			op.reqStore.outstandingRequests.empty()
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

		return op.resubmitOutstandingReqs()
	case stateUpdatedEvent:
		// When the state is updated, clear any outstanding requests, they may have been processed while we were gone
		op.reqStore = newRequestStore()
		return op.pbft.ProcessEvent(event)
	default:
		return op.pbft.ProcessEvent(event)
	}

	return nil
}
/*
func (op *obcBatch) startBatchTimer() {
	op.batchTimer.Reset(op.batchTimeout, batchTimerEvent{})
	logger.Debugf("Replica %d started the batch timer", op.pbft.id)
	op.batchTimerActive = true
}

func (op *obcBatch) stopBatchTimer() {
	op.batchTimer.Stop()
	logger.Debugf("Replica %d stopped the batch timer", op.pbft.id)
	op.batchTimerActive = false
}
*/
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
