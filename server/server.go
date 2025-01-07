package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ResponseMessage struct {
	Acknowledged bool
}

func init() {
	gob.Register(ResponseMessage{})
	gob.Register(Transaction{})

	gob.Register(Server{})
	gob.Register(FetchDataStoreRequest{})
	gob.Register(FetchDataStoreResponse{})
}

const (
	ClientsPerCluster = 1000
	Clusters          = 3
	ShardsPerCluster  = 4 // Doubt*****
	TotalShards       = Clusters * ShardsPerCluster
)

type PrePrepareMessage struct {
	SequenceNumber int
	Transaction    Transaction
	LeaderID       int
}

type PreparePBFTMessage struct {
	SequenceNumber int
	SenderID       int
	Transaction    Transaction
}

type PreparedCertificate struct {
	SequenceNumber int
	Transaction    Transaction
	LeaderID       int
}

type CommitPBFTMessage struct {
	SequenceNumber int
	SenderID       int
	Transaction    Transaction
}

type CommittedMessage struct {
	SequenceNumber int
	Transaction    Transaction
	LeaderID       int
}

type PrepareMessage struct {
    TxnID      int64
    SenderID   string
    ReceiverID string
    Amount     int
    FromCluster int 
    ToCluster   int 
}
//-----------------------------------------------------------

type FetchDataStoreRequest struct {
	ServerID     int
	LenDatastore int
}
type FetchDataStoreResponse struct {
	PendingTransaction map[int64]Transaction
}

type NetworkStatus struct {
	ActiveServers    map[string]bool
	ContactServers   map[string]bool
	ByzantineServers map[string]bool
}

type Transaction struct {
	Txnid    int64  `json:"txnid"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Amount   int    `json:"amount"`
}

type DataStoreTxn struct {
	Txn    Transaction `json:"txn"`
	Status string      `json:"status"`
}
type Server struct {
	Name               string
	Port               string
	ServerID           int
	CurrentView        int
	SequenceNumber     int
	isLeader           bool
	Db_path            string
	ClientBalances     map[string]int
	ClientLocks        map[string]bool
	ClusterId          int
	ClusterServersPort []string
	ActiveServers      map[string]bool
	ContactServers     map[string]bool
	ByzantineServers   map[string]bool
	// AcceptVal             Transaction
	WAL              map[int64]Transaction
	PromisesReceived int
	ExecutedTxn      map[int64]Transaction
	OngoingTxn       Transaction
	ActiveStatus     map[string]bool
	Mutex            sync.Mutex
	// Wg                    sync.WaitGroup
	Highest_Len_Server_ID int
	processedTxns         map[int64]bool
	SequenceNumberLock    sync.Mutex
	preCommitSentMap      map[int]bool
	commitSentMap         map[int]bool
	prepareRepliesMap     map[int]int
	commitRepliesMap      map[int]int
}

func getClusterForID(id int) int {
	if id >= 1 && id <= 1000 {
		return 1
	} else if id >= 1001 && id <= 2000 {
		return 2
	} else if id >= 2001 && id <= 3000 {
		return 3
	} else {
		return 0 // invalid ID
	}
}
func getClustersForTransaction(txn Transaction) []int {
	clusters := make(map[int]bool)

	senderID, err := strconv.Atoi(txn.Sender)
	if err == nil {
		cluster := getClusterForID(senderID)
		if cluster != 0 {
			clusters[cluster] = true
		}
	}

	receiverID, err := strconv.Atoi(txn.Receiver)
	if err == nil {
		cluster := getClusterForID(receiverID)
		if cluster != 0 {
			clusters[cluster] = true
		}
	}

	clustersList := []int{}
	for cluster := range clusters {
		clustersList = append(clustersList, cluster)
	}

	return clustersList
}

func (s *Server) HandleRequest(args *string, reply *string) error {
	*reply = "Response from server " + s.Name + " on port " + s.Port
	return nil
}

func (s *Server) AbortCrossShard(txnId int64, reply *bool) error {
	log.Printf("[%s] Abort Cross Shard Txn", s.Name)

	if _, exist := s.ClientLocks[s.WAL[txnId].Sender]; exist {

		s.ClientLocks[s.WAL[txnId].Sender] = false
	}
	if _, exist := s.ClientLocks[s.WAL[txnId].Receiver]; exist {
		s.ClientLocks[s.WAL[txnId].Receiver] = false
	}
	delete(s.WAL, txnId)

	*reply = false
	return nil

}

func (s *Server) CommitCrossShard(txnId int64, reply *bool) error {
	log.Printf("[%s] Committing Cross-Shard Txn %d", s.Name, txnId)

	txn, exists := s.WAL[txnId]
	if !exists {
		log.Printf("[%s] Transaction %d not found in WAL", s.Name, txnId)
		*reply = false
		return nil
	}


	s.ExecutedTxn[txn.Txnid] = txn

	s.releaseClients(txn)

	involvedClusters := getClustersForTransaction(txn)
	for _, clusterID := range involvedClusters {
		if clusterID != s.ClusterId+1 { // Send to other cluster(s)
			var ack bool
			err := s.SendCommitMessageToCluster(txn, clusterID, &ack)
			if err != nil {
				log.Printf("[%s] Error sending commit message to Cluster %d: %v", s.Name, clusterID, err)
			}
		}
	}

	*reply = true
	return nil
}

func (s *Server) HandleClientRequest(txn Transaction, reply *bool) error {
	s.Mutex.Lock()

	log.Printf("SERVER %s RECEIVED REQUEST FROM CLIENT: %+v", s.Name, txn)

	involvedClusters := getClustersForTransaction(txn)
	if len(involvedClusters) == 2 {

		if s.isLeader {
			senderID, err := strconv.Atoi(txn.Sender)
			if err != nil {
				log.Printf("[%s] Invalid sender ID '%s'. Aborting txn %d.", s.Name, txn.Sender, txn.Txnid)
				*reply = false
				return nil
			}

			balance, err := s.fetchClientBalance(senderID)
			if err != nil {
				log.Printf("[%s] Error fetching balance for sender %d: %v. Aborting txn %d.", s.Name, senderID, err, txn.Txnid)
				*reply = false
				return nil
			}

			if balance < txn.Amount {
				log.Printf("[%s] Insufficient balance for sender %d. Available: %d, Required: %d. Aborting txn %d.",
					s.Name, senderID, balance, txn.Amount, txn.Txnid)

				*reply = false
				return nil
			}

			s.initiatePbft(txn)
			*reply = true
			return nil
		} else {
			log.Printf("[%s] Not the leader for cross-shard transaction. Ignoring transaction.", s.Name)
			*reply = false
			return nil
		}

	} else if len(involvedClusters) == 1 {
		// log.Printf("Intra Shard Transactions: %v", txn)
		clusterID := involvedClusters[0]
		if s.ClusterId+1 == clusterID && s.isLeader {

			senderID, err := strconv.Atoi(txn.Sender)
			if err != nil {
				log.Printf("[%s] Invalid sender ID '%s'. Aborting txn %d.", s.Name, txn.Sender, txn.Txnid)
				// s.Wg.Done()
			} else {
				balance, err := s.fetchClientBalance(senderID)
				if err != nil {
					log.Printf("[%s] Error fetching balance for sender %d: %v. Aborting txn %d.", s.Name, senderID, err, txn.Txnid)
					// s.Wg.Done()
				} else if balance < txn.Amount {
					log.Printf("[%s] Insufficient balance for sender %d. Available: %d, Required: %d. Aborting txn %d.",
						s.Name, senderID, balance, txn.Amount, txn.Txnid)
					// s.Wg.Done()
				} else {
					s.initiatePbft(txn)
				}
			}
		} else {
			log.Printf("[%s] Not the leader for cluster %d. Ignoring transaction.", s.Name, clusterID)
			// s.Wg.Done()
		}
	} else {
		log.Printf("[%s] Invalid cluster involvement for transaction: %v", s.Name, txn)
		// s.Wg.Done()
	}

	s.Mutex.Unlock()
	// s.Wg.Wait()

	*reply = true
	return nil
}

func (s *Server) getNextSequenceNumber() int {
	s.SequenceNumberLock.Lock()
	defer s.SequenceNumberLock.Unlock()
	s.SequenceNumber++
	return s.SequenceNumber
}

func (s *Server) lockClients(txn Transaction) bool {

	if s.ClientLocks[txn.Sender] || s.ClientLocks[txn.Receiver] {
		return false
	}

	s.ClientLocks[txn.Sender] = true
	s.ClientLocks[txn.Receiver] = true
	return true
}

func (s *Server) releaseClients(txn Transaction) {

	if _, exist := s.ClientLocks[txn.Sender]; exist {
		s.ClientLocks[txn.Sender] = false
	}
	if _, exist := s.ClientLocks[txn.Receiver]; exist {
		s.ClientLocks[txn.Receiver] = false
	}
}

func (s *Server) initiatePbft(txn Transaction) {

	log.Printf("[%s] Leader initiating pbft for txn sender: %s, receiver: %s and amount: %d ", s.Name, txn.Sender, txn.Receiver, txn.Amount)

	sequenceNumber := s.getNextSequenceNumber()
	s.preCommitSentMap[sequenceNumber] = false
	s.commitSentMap[sequenceNumber] = false

	prePrepareMsg := PrePrepareMessage{
		SequenceNumber: sequenceNumber,
		Transaction:    txn,
		LeaderID:       s.ServerID,
	}

	inactiveserver := 0
	for _, addr := range s.ClusterServersPort {
		if addr == "" {
			continue
		}
		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var ack ResponseMessage
			err = client.Call("Server.ReceivePrePrepare", prePrepareMsg, &ack)
			if err != nil {

				inactiveserver++
				if inactiveserver == 2 {
					// s.Wg.Done()
				}
			}
		}(addr)
	}
}

func (s *Server) ReceivePrePrepare(prePreMsg PrePrepareMessage, ack *ResponseMessage) error {

	if s.ByzantineServers[s.Name] {
		log.Printf("[%s] Server is byzantine, not sending prepare message", s.Name)
		return nil
	}

	if !s.ActiveServers[s.Name] {
		log.Printf("[%s] Server is inactive, not sending prepare message", s.Name)
		return nil
	}

	log.Printf("[%s] received pre-prepare from Leader for txn sender: %s, receiver: %s and amount: %d ", s.Name, prePreMsg.Transaction.Sender, prePreMsg.Transaction.Receiver, prePreMsg.Transaction.Amount)


	prepareMsg := PreparePBFTMessage{
		SequenceNumber: prePreMsg.SequenceNumber,
		SenderID:       s.ServerID,
		Transaction:    prePreMsg.Transaction,
	}

	leaderAddr := fmt.Sprintf("localhost:80%02d", prePreMsg.LeaderID)
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		ack.Acknowledged = false
		return nil
	}
	defer client.Close()

	var prepAck ResponseMessage
	err = client.Call("Server.ReceivePrepare", prepareMsg, &prepAck)
	if err != nil {
		ack.Acknowledged = false
		return nil
	}

	ack.Acknowledged = true
	return nil
}

func (s *Server) ReceivePrepare(prepareMsg PreparePBFTMessage, ack *ResponseMessage) error {

	log.Printf("[%s] Received prepare message from %d", s.Name, prepareMsg.SenderID)

	s.prepareRepliesMap[prepareMsg.SequenceNumber]++
	ack.Acknowledged = true

	majority := 2

	sent, exists := s.preCommitSentMap[prepareMsg.SequenceNumber]
	if !exists {
		s.preCommitSentMap[prepareMsg.SequenceNumber] = false
		sent = false
	}

	if !sent && s.prepareRepliesMap[prepareMsg.SequenceNumber] >= majority {

		log.Printf("[%s] Majority reached, attempting to lock clients for txn %d", s.Name, prepareMsg.Transaction.Txnid)
		if !s.lockClients(prepareMsg.Transaction) {
			log.Printf("[%s] Could not acquire locks for txn %d, waiting for locks", s.Name, prepareMsg.Transaction.Txnid)
			return nil
		}
		log.Printf("[%s] Locks acquired, sending PreCommitMessage now for txn %d", s.Name, prepareMsg.Transaction.Txnid)

		s.preCommitSentMap[prepareMsg.SequenceNumber] = true

		preparedCertificate := PreparedCertificate{
			SequenceNumber: prepareMsg.SequenceNumber,
			Transaction:    prepareMsg.Transaction,
			LeaderID:       s.ServerID,
		}

		for _, addr := range s.ClusterServersPort {
			if addr == "" {
				continue
			}
			go func(addr string) {
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer client.Close()

				var commitAck ResponseMessage

				err = client.Call("Server.ReceivePreparedCertificate", preparedCertificate, &commitAck)
				if err != nil {
					log.Printf("Error calling ReceivePreCommit on %s: %v", addr, err)
				}
			}(addr)
		}
	} else {
		log.Printf("Quorum not reached!!!!")
	}

	return nil
}

func (s *Server) ReceivePreparedCertificate(preCommMsg PreparedCertificate, ack *ResponseMessage) error {

	if s.ByzantineServers[s.Name] {
		log.Printf("[%s] Server is byzantine, not sending commit message", s.Name)
		return nil
	}

	if !s.ActiveServers[s.Name] {
		log.Printf("[%s] Server is inactive, not sending commit message", s.Name)
		return nil
	}

	log.Printf("[%s] Received pre-commit message from the leader", s.Name)

	if !s.lockClients(preCommMsg.Transaction) {
		log.Printf("[%s] Could not acquire locks for txn %d, waiting for locks", s.Name, preCommMsg.Transaction.Txnid)
		ack.Acknowledged = false
		return nil
	}

	commitMsg := CommitPBFTMessage{
		SequenceNumber: preCommMsg.SequenceNumber,
		SenderID:       s.ServerID,
		Transaction:    preCommMsg.Transaction,
	}

	leaderAddr := fmt.Sprintf("localhost:80%02d", preCommMsg.LeaderID)
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		ack.Acknowledged = false
		return nil
	}
	defer client.Close()

	var commitAck ResponseMessage
	err = client.Call("Server.ReceiveCommitPBFT", commitMsg, &commitAck)
	if err != nil {
		ack.Acknowledged = false
		return nil
	}

	ack.Acknowledged = true
	return nil
}

func (s *Server) ReceiveCommitPBFT(commitMsg CommitPBFTMessage, ack *ResponseMessage) error {

	log.Printf("[%s] Received commit message from %d", s.Name, commitMsg.SenderID)

	s.commitRepliesMap[commitMsg.SequenceNumber]++
	ack.Acknowledged = true

	majority := 2

	sent, exists := s.commitSentMap[commitMsg.SequenceNumber]
	if !exists {
		s.commitSentMap[commitMsg.SequenceNumber] = false
		sent = false
	}

	if !sent && s.commitRepliesMap[commitMsg.SequenceNumber] >= majority {

		s.commitSentMap[commitMsg.SequenceNumber] = true
		txn := commitMsg.Transaction
		sender, _ := strconv.Atoi(txn.Sender)
		receiver, _ := strconv.Atoi(txn.Receiver)

		senderBalance, err := s.fetchClientBalance(sender)
		if err != nil {
			log.Printf("[%s] Error fetching sender balance: %v", s.Name, err)
		}

		receiverBalance, err := s.fetchClientBalance(receiver)
		if err != nil {
			log.Printf("[%s] Error fetching receiver balance: %v", s.Name, err)
		}

		err = s.updateClientBalance(sender, senderBalance-txn.Amount)
		if err != nil {
			log.Printf("[%s] Error updating sender balance: %v", s.Name, err)
		}

		err = s.updateClientBalance(receiver, receiverBalance+txn.Amount)
		if err != nil {
			log.Printf("[%s] Error updating receiver balance: %v", s.Name, err)
		}

		log.Printf("[%s] Successfully committing txn, sender: %s, receiver: %s, amount: %d", s.Name, commitMsg.Transaction.Sender, commitMsg.Transaction.Receiver, commitMsg.Transaction.Amount)


		newTxn := DataStoreTxn{
			Txn:    txn,
			Status: "Committed",
		}
		err = s.appendToClientDataStore(sender, newTxn)
		if err != nil {
			log.Printf("[%s] Error adding txn to sender's DataStore: %v", s.Name, err)
		}


		err = s.appendToClientDataStore(receiver, newTxn)
		if err != nil {
			log.Printf("[%s] Error adding txn to receiver's DataStore: %v", s.Name, err)
		}

		committedMsg := CommittedMessage{
			SequenceNumber: commitMsg.SequenceNumber,
			Transaction:    txn,
			LeaderID:       s.ServerID,
		}

		for _, addr := range s.ClusterServersPort {
			if addr == "" {
				continue
			}
			go func(addr string) {
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer client.Close()

				var finalAck ResponseMessage
				client.Call("Server.ReceiveCommitted", committedMsg, &finalAck)
			}(addr)
		}
		s.releaseClients(txn)
		// s.Wg.Done()
	}

	return nil
}



func (s *Server) ReceiveCommitted(committedMsg CommittedMessage, ack *ResponseMessage) error {

	log.Printf("[%s] Received committed message from leader", s.Name)

	if s.ByzantineServers[s.Name] {
		log.Printf("[%s] Server is byzantine, not executing txn", s.Name)
		return nil
	}

	if !s.ActiveServers[s.Name] {
		log.Printf("[%s] Server is inactive, not executing txn", s.Name)
		return nil
	}

	txn := committedMsg.Transaction
	sender, _ := strconv.Atoi(txn.Sender)
	receiver, _ := strconv.Atoi(txn.Receiver)

	senderBalance, err := s.fetchClientBalance(sender)
	if err != nil {
		log.Printf("[%s] Error fetching sender balance: %v", s.Name, err)
	}

	receiverBalance, err := s.fetchClientBalance(receiver)
	if err != nil {
		log.Printf("[%s] Error fetching receiver balance: %v", s.Name, err)
	}

	err = s.updateClientBalance(sender, senderBalance-txn.Amount)
	if err != nil {
		log.Printf("[%s] Error updating sender balance: %v", s.Name, err)
	}

	err = s.updateClientBalance(receiver, receiverBalance+txn.Amount)
	if err != nil {
		log.Printf("[%s] Error updating receiver balance: %v", s.Name, err)
	}

	log.Printf("[%s] Successfully committing txn, sender: %s, receiver: %s, amount: %d", s.Name, committedMsg.Transaction.Sender, committedMsg.Transaction.Receiver, committedMsg.Transaction.Amount)


	newTxn := DataStoreTxn{
		Txn:    txn,
		Status: "Committed",
	}
	err = s.appendToClientDataStore(sender, newTxn)
	if err != nil {
		log.Printf("[%s] Error adding txn to sender's DataStore: %v", s.Name, err)
	}


	err = s.appendToClientDataStore(receiver, newTxn)
	if err != nil {
		log.Printf("[%s] Error adding txn to receiver's DataStore: %v", s.Name, err)
	}

	s.ExecutedTxn[txn.Txnid] = txn

	if _, exist := s.ClientLocks[txn.Sender]; exist {
		s.ClientLocks[txn.Sender] = false
	}
	if _, exist := s.ClientLocks[txn.Receiver]; exist {
		s.ClientLocks[txn.Receiver] = false
	}

	s.releaseClients(txn)

	ack.Acknowledged = true
	return nil
}


const timeoutDuration = 2 * time.Second


func (s *Server) SendPrepareMessage(txn Transaction, toCluster int, reply *bool) error {
	prepareMsg := PrepareMessage{
		TxnID:       txn.Txnid,
		SenderID:    txn.Sender,
		ReceiverID:  txn.Receiver,
		Amount:      txn.Amount,
		FromCluster: s.ClusterId,
		ToCluster:   toCluster,
	}

	ackChan := make(chan bool, 1)

	go func() {

		targetServer := fmt.Sprintf("localhost:80%02d", (toCluster-1)*4+1)
		client, err := rpc.Dial("tcp", targetServer)
		if err != nil {
			log.Printf("[%s] Error connecting to cluster %d: %v", s.Name, toCluster, err)
			ackChan <- false
			return
		}
		defer client.Close()

		var ack ResponseMessage
		err = client.Call("Server.ReceivePrepareMessage", prepareMsg, &ack)
		if err != nil {
			log.Printf("[%s] Error sending PrepareMessage to cluster %d: %v", s.Name, toCluster, err)
			ackChan <- false
			return
		}
		ackChan <- ack.Acknowledged
	}()

	select {
	case ack := <-ackChan:
		if ack {
			log.Printf("[%s] Prepare message acknowledged by cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
			*reply = true
		} else {
			log.Printf("[%s] Prepare message not acknowledged by cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
			*reply = false
		}
	case <-time.After(timeoutDuration):
		log.Printf("[%s] Timeout occurred while waiting for PrepareMessage acknowledgment from cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
		*reply = false
	}
	return nil
}


func (s *Server) SendCommitMessageToCluster(txn Transaction, toCluster int, reply *bool) error {
	commitMsg := CommitPBFTMessage{
		SequenceNumber: int(txn.Txnid),
		SenderID:       s.ServerID,
		Transaction:    txn,
	}

	ackChan := make(chan bool, 1)

	go func() {

		targetServer := fmt.Sprintf("localhost:80%02d", (toCluster-1)*4+1)
		client, err := rpc.Dial("tcp", targetServer)
		if err != nil {
			log.Printf("[%s] Error connecting to cluster %d: %v", s.Name, toCluster, err)
			ackChan <- false
			return
		}
		defer client.Close()

		var ack ResponseMessage
		err = client.Call("Server.ReceiveCommitPBFT", commitMsg, &ack)
		if err != nil {
			log.Printf("[%s] Error sending CommitMessage to cluster %d: %v", s.Name, toCluster, err)
			ackChan <- false
			return
		}
		ackChan <- ack.Acknowledged
	}()

	select {
	case ack := <-ackChan:
		if ack {
			log.Printf("[%s] Commit message acknowledged by cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
			*reply = true
		} else {
			log.Printf("[%s] Commit message not acknowledged by cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
			*reply = false
		}
	case <-time.After(timeoutDuration):
		log.Printf("[%s] Timeout occurred while waiting for CommitMessage acknowledgment from cluster %d for TxnID %d", s.Name, toCluster, txn.Txnid)
		*reply = false
	}
	return nil
}


func (s *Server) ReceivePrepareMessage(prepareMsg PrepareMessage, ack *ResponseMessage) error {
    log.Printf("[%s] Received PrepareMessage from Cluster %d for TxnID %d", s.Name, prepareMsg.FromCluster, prepareMsg.TxnID)

    if !s.lockClients(Transaction{Sender: prepareMsg.SenderID, Receiver: prepareMsg.ReceiverID, Amount: prepareMsg.Amount}) {
        log.Printf("[%s] Could not acquire lock for receiver %s. Aborting TxnID %d", s.Name, prepareMsg.ReceiverID, prepareMsg.TxnID)
        ack.Acknowledged = false
        return nil
    }

    receiverID, err := strconv.Atoi(prepareMsg.ReceiverID)
    if err != nil {
        log.Printf("[%s] Invalid receiver ID '%s'. Aborting TxnID %d", s.Name, prepareMsg.ReceiverID, prepareMsg.TxnID)
        ack.Acknowledged = false
        return nil
    }

    balance, err := s.fetchClientBalance(receiverID)
    if err != nil {
        log.Printf("[%s] Error fetching balance for receiver %d: %v. Aborting TxnID %d", s.Name, receiverID, err, prepareMsg.TxnID)
        ack.Acknowledged = false
        return nil
    }

    log.Printf("[%s] Receiver %d has balance: %d. Prepare phase for TxnID %d successful.", s.Name, receiverID, balance, prepareMsg.TxnID)
    ack.Acknowledged = true
    return nil
}



func hashCommitMessage(commitMsg CommitPBFTMessage) string {
	data := fmt.Sprintf("%d:%d:%s:%s:%d",
		commitMsg.SequenceNumber,
		commitMsg.SenderID,
		commitMsg.Transaction.Sender,
		commitMsg.Transaction.Receiver,
		commitMsg.Transaction.Amount,
	)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (s *Server) ReceiveHashedCommit(data struct {
	CommitMsg    CommitPBFTMessage
	HashedCommit string
}, ack *ResponseMessage) error {
	log.Printf("[%s] Received hashed commit message for Txn ID %d", s.Name, data.CommitMsg.Transaction.Txnid)

	computedHash := hashCommitMessage(data.CommitMsg)
	if computedHash != data.HashedCommit {
		log.Printf("[%s] Hash mismatch! Received: %s, Computed: %s", s.Name, data.HashedCommit, computedHash)
		ack.Acknowledged = false
		return fmt.Errorf("hash mismatch for commit message")
	}

	log.Printf("[%s] Hash verified successfully for Txn ID %d", s.Name, data.CommitMsg.Transaction.Txnid)

	err := s.ReceiveCommitPBFT(data.CommitMsg, ack)
	if err != nil {
		log.Printf("[%s] Error processing commit message: %v", s.Name, err)
		return err
	}

	ack.Acknowledged = true
	return nil
}

func (s *Server) fetchClientBalance(clientID int) (int, error) {
	dbPath := s.Db_path

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return 0, fmt.Errorf("database file does not exist: %s", dbPath)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	var amount int
	query := `SELECT Amount FROM clients WHERE ClientID = ?;`
	err = db.QueryRow(query, clientID).Scan(&amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("client with ID %d not found", clientID)
		}
		return 0, fmt.Errorf("failed to fetch balance for client %d: %v", clientID, err)
	}

	return amount, nil
}


func (s *Server) updateClientBalance(clientID int, newAmount int) error {
	dbPath := s.Db_path

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database file does not exist: %s", dbPath)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %v", dbPath, err)
	}

	updateSQL := `UPDATE clients SET Amount = ? WHERE ClientID = ?;`
	result, err := tx.Exec(updateSQL, newAmount, clientID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update balance for client %d: %v", clientID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to get rows affected for client %d: %v", clientID, err)
	}

	if rowsAffected == 0 {
		tx.Rollback()
		return fmt.Errorf("client with ID %d not found", clientID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %v", dbPath, err)
	}

	return nil
}

func (s *Server) fetchClientDataStore(clientID int) ([]DataStoreTxn, error) {
	dbPath := s.Db_path


	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", dbPath)
	}


	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	var dataStoreStr string
	query := `SELECT DataStore FROM clients WHERE ClientID = ?;`
	err = db.QueryRow(query, clientID).Scan(&dataStoreStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("client with ID %d not found", clientID)
		}
		return nil, fmt.Errorf("failed to fetch DataStore for client %d: %v", clientID, err)
	}

	var dataStore []DataStoreTxn
	if dataStoreStr == "" || dataStoreStr == "[]" {
		dataStore = []DataStoreTxn{}
	} else {
		err = json.Unmarshal([]byte(dataStoreStr), &dataStore)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal DataStore for client %d: %v", clientID, err)
		}
	}

	return dataStore, nil
}

func (s *Server) appendToClientDataStore(clientID int, newTxn DataStoreTxn) error {
	dataStore, err := s.fetchClientDataStore(clientID)
	if err != nil {
		return err
	}
	dataStore = append(dataStore, newTxn)

	dataStoreBytes, err := json.Marshal(dataStore)
	if err != nil {
		return fmt.Errorf("failed to marshal DataStore for client %d: %v", clientID, err)
	}


	dbPath := s.Db_path


	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %v", dbPath, err)
	}

	updateSQL := `UPDATE clients SET DataStore = ?;`
	result, err := tx.Exec(updateSQL, string(dataStoreBytes))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update DataStore for client %d: %v", clientID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to get rows affected for client %d: %v", clientID, err)
	}

	if rowsAffected == 0 {
		tx.Rollback()
		return fmt.Errorf("client with ID %d not found", clientID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %v", dbPath, err)
	}

	return nil
}

func (s *Server) GetDataStore(request *FetchDataStoreRequest, response *FetchDataStoreResponse) error {

	response.PendingTransaction = s.ExecutedTxn

	return nil
}

func (s *Server) HandleNetworkStatus(networkStatus NetworkStatus, reply *string) error {

	s.ActiveServers = networkStatus.ActiveServers
	s.ActiveStatus = networkStatus.ActiveServers

	s.ContactServers = networkStatus.ContactServers
	s.ByzantineServers = networkStatus.ByzantineServers

	// log.Printf("[%s] UPDATED NETWORK STATUS: ActiveServers: %v, ContactServers: %v, ByzantineServers: %v", s.Name, s.ActiveServers, s.ContactServers, s.ByzantineServers)

	return nil
}

func startServer(s *Server, wg *sync.WaitGroup) {
	defer wg.Done()

	serverName := s.Name
	var serverID int
	switch serverName {
	case "S1":
		serverID = 1
	case "S2":
		serverID = 2
	case "S3":
		serverID = 3
	case "S4":
		serverID = 4
	case "S5":
		serverID = 5
	case "S6":
		serverID = 6
	case "S7":
		serverID = 7
	case "S8":
		serverID = 8
	case "S9":
		serverID = 9
	case "S10":
		serverID = 10
	case "S11":
		serverID = 11
	case "S12":
		serverID = 12
	default:
		log.Fatalf("Invalid server name: %s.", serverName)
		return
	}

	s.processedTxns = make(map[int64]bool)
	s.preCommitSentMap = make(map[int]bool)
	s.commitSentMap = make(map[int]bool)
	s.prepareRepliesMap = make(map[int]int)
	s.commitRepliesMap = make(map[int]int)

	s.ServerID = serverID
	s.ClusterId = (serverID - 1) / 4
	s.Db_path = initializeShard(s.ClusterId+1, serverID)

	// setting leader based on cluster id and server id
	switch s.ClusterId {
	case 0: // Cluster 1
		if s.ServerID == 1 {
			s.isLeader = true
		}
	case 1: // Cluster 2
		if s.ServerID == 5 {
			s.isLeader = true
		}
	case 2: // Cluster 3
		if s.ServerID == 9 {
			s.isLeader = true
		}
	}

	for i := 0; i < 4; i++ {
		if (4*s.ClusterId + i + 1) != s.ServerID {
			s.ClusterServersPort = append(s.ClusterServersPort, fmt.Sprintf("localhost:80%02d", (4*s.ClusterId+i+1)))
		} else {
			s.ClusterServersPort = append(s.ClusterServersPort, "")
		}
	}

	s.ClientBalances = make(map[string]int)
	s.ClientLocks = make(map[string]bool)
	s.ExecutedTxn = make(map[int64]Transaction)
	s.WAL = make(map[int64]Transaction)
	for idx := range 1000 {
		s.ClientBalances[fmt.Sprintf("%d", s.ClusterId*1000+idx+1)] = 10
		s.ClientLocks[fmt.Sprintf("%d", s.ClusterId*1000+idx+1)] = false
	}

	server := rpc.NewServer()

	server.RegisterName("Server", s)
	// log.Print(s)
	listener, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		log.Fatalf("Error starting server %s: %v", s.Name, err)
	}
	defer listener.Close()

	fmt.Printf("Server %s listening on port %s\n", s.Name, s.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Server %s: Error accepting connection: %v", s.Name, err)
			continue
		}

		go server.ServeConn(conn)
	}

}

func getClusterRange(cluster int) (int, int) {
	startID := (cluster-1)*1000 + 1
	endID := cluster * 1000
	return startID, endID
}

func initializeShard(cluster, shard int) string {
	shardName := fmt.Sprintf("database_C%d_S%d.db", cluster, shard)
	dbPath := path.Join("database", shardName)

	os.MkdirAll("database", os.ModePerm)

	db, _ := sql.Open("sqlite3", dbPath)

	defer db.Close()

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS clients (
		ClientID INTEGER PRIMARY KEY,
		Amount INTEGER NOT NULL,
		DataStore TEXT,
		WAL TEXT
	);
	`
	_, _ = db.Exec(createTableSQL)

	startID, endID := getClusterRange(cluster)

	tx, _ := db.Begin()

	updateSQL := `INSERT OR REPLACE INTO clients ( ClientID, Amount, DataStore, WAL) VALUES (?,?,?,?);`

	for clientID := startID; clientID <= endID; clientID++ {
		_, err := tx.Exec(updateSQL, clientID, 10, "[]", "[]")


		if err != nil {
			log.Print(err)
		}
	}

	_ = tx.Commit()


	return dbPath
}

func (s *Server) GetClientBalance(clientID int, balance *int) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	amount, err := s.fetchClientBalance(clientID)
	if err != nil {
		*balance = 0
		return nil
	}
	*balance = amount
	return nil
}

func (s *Server) GetCommittedTransactions(_ struct{}, committedTxns *[]Transaction) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	var txns []Transaction

	for _, txn := range s.ExecutedTxn {
		txns = append(txns, txn)
	}

	*committedTxns = txns
	return nil
}

func main() {
	var wg sync.WaitGroup

	servers := []Server{
		{Name: "S1", Port: "8001"},
		{Name: "S2", Port: "8002"},
		{Name: "S3", Port: "8003"},
		{Name: "S4", Port: "8004"},
		{Name: "S5", Port: "8005"},
		{Name: "S6", Port: "8006"},
		{Name: "S7", Port: "8007"},
		{Name: "S8", Port: "8008"},
		{Name: "S9", Port: "8009"},
		{Name: "S10", Port: "8010"},
		{Name: "S11", Port: "8011"},
		{Name: "S12", Port: "8012"},
	}

	for i := range servers {
		wg.Add(1)

		go startServer(&servers[i], &wg)
	}

	wg.Wait()
}
