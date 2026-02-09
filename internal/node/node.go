package node

import (
	"HM2/internal/inmemory"
	"HM2/internal/protocol"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"
)

const (
	DefaultLeaderID          = "A"
	DefaultReplicationMode   = protocol.ReplicationSync
	DefaultReplicationFactor = 1
	DefaultAckNeed           = 1
	DefaultMinDelayMs        = 0
	DefaultMaxDelayMs        = 0
)

type Node struct {
	id       string
	hostname string
	port     int

	storage *inmemory.Storage
	dedup   *inmemory.Deduplication

	mu sync.RWMutex

	nodes       map[string]protocol.NodeInfo
	connections map[string]net.Conn
	handlers    map[string]func([]byte, *bufio.Writer) (error, uuid.UUID)

	leaderID        string
	replicationMode string
	rf              int
	k               int
	delayMinMs      int
	delayMaxMs      int
}

func NewNode(id, hostname string, port int) *Node {
	node := &Node{
		id:              id,
		hostname:        hostname,
		port:            port,
		storage:         inmemory.NewStorage(),
		dedup:           inmemory.NewDeduplication(),
		nodes:           make(map[string]protocol.NodeInfo),
		connections:     make(map[string]net.Conn),
		leaderID:        DefaultLeaderID,
		replicationMode: DefaultReplicationMode,
		rf:              DefaultReplicationFactor,
		k:               DefaultAckNeed,
		delayMinMs:      DefaultMinDelayMs,
		delayMaxMs:      DefaultMaxDelayMs,
	}
	node.handlers = map[string]func([]byte, *bufio.Writer) (error, uuid.UUID){
		protocol.TypeClientGetRequest:  node.handlerClientGetRequest,
		protocol.TypeClientPutRequest:  node.handlerClientPutRequest,
		protocol.TypeClientDumpRequest: node.handlerClientDumpRequest,
		protocol.TypeReplicationPut:    node.handlerReplicationPut,
		protocol.TypeClusterUpdate:     node.handlerClusterUpdate,
	}
	return node

}

func (node *Node) Start() error {
	node.dedup.StartVacuum()

	address := net.JoinHostPort(node.hostname, strconv.Itoa(node.port))
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return fmt.Errorf("listen %s: %w", address, err)
	}

	slog.Info("node started", "id", node.id, "addr", address)

	for {
		connection, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go node.handleConn(connection)
	}
}

func (node *Node) IsLeader() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()

	return node.id == node.leaderID
}

func (node *Node) SelfInfo() protocol.NodeInfo {
	return protocol.NodeInfo{
		ID:       node.id,
		Hostname: node.hostname,
		Port:     node.port,
	}
}

func (node *Node) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			slog.Warn("read failed", "remote", conn.RemoteAddr().String(), "error", err)
			return
		}

		var env struct {
			Type string `json:"type"`
		}

		if err := json.Unmarshal(line, &env); err != nil {
			response := protocol.NewClientResponse(uuid.Nil, node.SelfInfo(), protocol.NewBadRequestError("bad json"))
			_ = node.writeJSONLine(writer, response)
			continue
		}

		if handler, ok := node.handlers[env.Type]; ok {
			if err, id := handler(line, writer); err != nil {
				var ae protocol.ApplicationError
				if errors.As(err, &ae) && node.isClientRequest(env.Type) {
					response := protocol.NewClientResponse(id, node.SelfInfo(), err)
					_ = node.writeJSONLine(writer, response)
				}
				slog.Warn("handle request failed", "type", env.Type, "error", err)
			}
		} else {
			slog.Warn("unknown type", "type", env.Type)
		}
	}
}

func (node *Node) writeJSONLine(writer *bufio.Writer, v any) error {
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	if err := writer.WriteByte('\n'); err != nil {
		return err
	}
	return writer.Flush()
}

func (node *Node) isClientRequest(requestType string) bool {
	return requestType == protocol.TypeClientGetRequest ||
		requestType == protocol.TypeClientPutRequest ||
		requestType == protocol.TypeClientDumpRequest
}

func (node *Node) applyClusterUpdate(clusterUpdate protocol.ClusterUpdate) error {
	//node.mu.Lock()
	//defer node.mu.Unlock()
	//
	//if node.IsLeader() {
	//	for _, connection := range node.connections {
	//		err := connection.Close()
	//
	//		if err != nil {
	//			slog.Error("Failed to close connection", "error", err)
	//		}
	//	}
	//}
	//
	//node.connections = make(map[string]net.Conn)
	//node.nodes = make(map[string]protocol.NodeInfo)
	//for _, nodeInfo := range clusterUpdate.Nodes {
	//	node.nodes[nodeInfo.ID] = nodeInfo
	//}
	//node.leaderID = clusterUpdate.LeaderID
	//node.replicationMode = clusterUpdate.ReplicationMode
	//node.rf = clusterUpdate.RF
	//node.k = clusterUpdate.K
	//node.delayMinMs = clusterUpdate.MinDelayMs
	//node.delayMaxMs = clusterUpdate.MaxDelayMs
	//
	//if node.IsLeader() {
	//
	//}

}

func (node *Node) handlerClientGetRequest(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ClientGetRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal client get request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	value, exists := node.storage.Get(request.Key)

	response := protocol.NewClientGetResponse(
		request.RequestUUID,
		node.SelfInfo(),
		value,
		exists,
		nil)

	err := node.writeJSONLine(writer, response)
	if err != nil {
		return fmt.Errorf("write response: %w", err), request.RequestUUID
	}
	return nil, request.RequestUUID
}

func (node *Node) handlerClientPutRequest(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ClientPutRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal client put request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	if !node.IsLeader() {
		return fmt.Errorf("not leader: %w", protocol.NewNotLeaderError(node.leaderID)), request.RequestUUID
	}

	node.storage.Put(request.Key, request.Value)
	// TODO replicate to followers

	response := protocol.NewClientPutResponse(
		request.RequestUUID,
		node.SelfInfo(),
		nil)

	err := node.writeJSONLine(writer, response)

	if err != nil {
		return fmt.Errorf("write response: %w", err), request.RequestUUID
	}
	return nil, request.RequestUUID
}

func (node *Node) handlerClientDumpRequest(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ClientDumpRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal client dump request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	dump := node.storage.Dump()

	response := protocol.NewClientDumpResponse(
		request.RequestUUID,
		node.SelfInfo(),
		dump,
		nil)

	err := node.writeJSONLine(writer, response)

	if err != nil {
		return fmt.Errorf("write response: %w", err), request.RequestUUID
	}

	return nil, request.RequestUUID
}

func (node *Node) handlerReplicationPut(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ReplicationPut
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal replication put request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	if !node.dedup.Exist(request.OperationID) {
		node.dedup.Add(request.OperationID)
		node.storage.Put(request.Key, request.Value)
	}

	response := protocol.NewReplicationAck(
		request.OperationID,
		node.SelfInfo(),
	)

	err := node.writeJSONLine(writer, response)

	if err != nil {
		return fmt.Errorf("write response: %w", err), request.OperationID
	}

	return nil, request.OperationID
}

func (node *Node) handlerClusterUpdate(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ClusterUpdate
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal cluster update request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	if err := request.Validate(); err != nil {
		return fmt.Errorf("invalid cluster update request: %w", protocol.NewBadRequestError("invalid cluster update request")), uuid.Nil
	}

	node.applyClusterUpdate(request)
}
