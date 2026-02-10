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

	"github.com/google/uuid"
)

type Node struct {
	id       string
	hostname string
	port     int

	storage *inmemory.Storage
	dedup   *inmemory.Deduplication

	peerManager   *PeerManager
	handlers      map[string]func([]byte, *bufio.Writer) (error, uuid.UUID)
	errorHandlers map[string]func(uuid.UUID, *bufio.Writer, error)
}

func NewNode(id, hostname string, port int) *Node {
	node := &Node{
		id:          id,
		hostname:    hostname,
		port:        port,
		storage:     inmemory.NewStorage(),
		dedup:       inmemory.NewDeduplication(),
		peerManager: NewPeerManager(protocol.NodeInfo{ID: id, Hostname: hostname, Port: port}),
	}
	node.handlers = map[string]func([]byte, *bufio.Writer) (error, uuid.UUID){
		protocol.TypeClientGetRequest:     node.handlerClientGetRequest,
		protocol.TypeClientPutRequest:     node.handlerClientPutRequest,
		protocol.TypeClientDumpRequest:    node.handlerClientDumpRequest,
		protocol.TypeReplicationPut:       node.handlerReplicationPut,
		protocol.TypeClusterUpdateRequest: node.handlerClusterUpdateRequest,
	}
	node.errorHandlers = map[string]func(uuid.UUID, *bufio.Writer, error){
		protocol.TypeClientGetRequest:     node.handlerClientError,
		protocol.TypeClientPutRequest:     node.handlerClientError,
		protocol.TypeClientDumpRequest:    node.handlerClientError,
		protocol.TypeClusterUpdateRequest: node.handlerClusterUpdateError,
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
				if errorHandler, ok := node.errorHandlers[env.Type]; ok {
					errorHandler(id, writer, err)
				} else {
					slog.Warn("unknown handle error type", "type", env.Type)
				}
				slog.Warn("handle request failed", "type", env.Type, "error", err)
			}
		} else {
			slog.Warn("unknown handle type", "type", env.Type)
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

	if !node.peerManager.IsLeader() {
		leaderID := node.peerManager.GetLeaderID()
		return fmt.Errorf("not leader: %w", protocol.NewNotLeaderError(leaderID)), request.RequestUUID
	}

	node.storage.Put(request.Key, request.Value)
	err := node.peerManager.Release(&request)

	if err != nil {
		return fmt.Errorf("release request: %w", err), request.RequestUUID
	}

	response := protocol.NewClientPutResponse(
		request.RequestUUID,
		node.SelfInfo(),
		nil)

	err = node.writeJSONLine(writer, response)

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

func (node *Node) handlerClusterUpdateRequest(data []byte, writer *bufio.Writer) (error, uuid.UUID) {
	var request protocol.ClusterUpdateRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("unmarshal cluster update request: %w", protocol.NewBadRequestError("bad json")), uuid.Nil
	}

	if err := request.Validate(); err != nil {
		return fmt.Errorf("invalid cluster update request: %w", err), request.RequestID
	}

	node.peerManager.ApplyClusterUpdate(&request)

	response := protocol.NewClusterUpdateResponse(
		request.RequestID,
		node.SelfInfo(),
		nil,
	)

	err := node.writeJSONLine(writer, response)

	if err != nil {
		return fmt.Errorf("write response: %w", err), request.RequestID
	}

	return nil, request.RequestID
}

func (node *Node) handlerClientError(requestId uuid.UUID, writer *bufio.Writer, error error) {
	response := protocol.NewClientResponse(requestId, node.SelfInfo(), error)
	_ = node.writeJSONLine(writer, response)
}

func (node *Node) handlerClusterUpdateError(requestId uuid.UUID, writer *bufio.Writer, error error) {
	response := protocol.NewClusterUpdateResponse(requestId, node.SelfInfo(), error)
	_ = node.writeJSONLine(writer, response)
}
