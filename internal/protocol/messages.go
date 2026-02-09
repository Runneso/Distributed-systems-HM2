package protocol

import (
	"errors"

	"github.com/google/uuid"
)

const (
	TypeClientPutRequest  = "CLIENT_PUT_REQUEST"
	TypeClientGetRequest  = "CLIENT_GET_REQUEST"
	TypeClientDumpRequest = "CLIENT_DUMP_REQUEST"

	TypeClientPutResponse  = "CLIENT_PUT_RESPONSE"
	TypeClientGetResponse  = "CLIENT_GET_RESPONSE"
	TypeClientDumpResponse = "CLIENT_DUMP_RESPONSE"

	TypeReplicationPut = "REPL_PUT"
	TypeReplicationAck = "REPL_ACK"

	TypeClusterUpdate = "CLUSTER_UPDATE"
)

const (
	StatusOK    = "OK"
	StatusError = "ERROR"

	ErrorNotLeader         = "NOT_LEADER"
	ErrorNotEnoughReplicas = "NOT_ENOUGH_REPLICAS"
	ErrorTimeout           = "TIMEOUT"
	ErrorBadRequest        = "BAD_REQUEST"
	ErrorUnknownNode       = "UNKNOWN_NODE"
)

const (
	ReplicationAsync    = "async"
	ReplicationSemiSync = "semi-sync"
	ReplicationSync     = "sync"
)

var allowedReplicationModes = map[string]struct{}{
	ReplicationAsync:    {},
	ReplicationSemiSync: {},
	ReplicationSync:     {},
}

type ClientError struct {
	Code    string
	Message string
}

func (clientError ClientError) Error() string {
	return clientError.Message
}

func (clientError ClientError) ErrorCode() string {
	return clientError.Code
}

func NewBadRequestError(message string) ClientError {
	return ClientError{
		Code:    ErrorBadRequest,
		Message: message,
	}
}

func NewNotLeaderError(leaderID string) ClientError {
	return ClientError{
		Code:    ErrorNotLeader,
		Message: "node is not leader, leader is " + leaderID,
	}
}

func NewNotEnoughReplicasError(msg string) ClientError {
	return ClientError{
		Code:    ErrorNotEnoughReplicas,
		Message: msg,
	}
}

func NewTimeoutError(msg string) ClientError {
	return ClientError{
		Code:    ErrorTimeout,
		Message: msg,
	}
}

func NewUnknownNodeError(msg string) ClientError {
	return ClientError{
		Code:    ErrorUnknownNode,
		Message: msg,
	}
}

type NodeInfo struct {
	ID       string `json:"node_id"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}

type ClusterUpdate struct {
	Type            string     `json:"type"`
	Nodes           []NodeInfo `json:"nodes"`
	LeaderID        string     `json:"leader_id"`
	ReplicationMode string     `json:"replication_mode"`
	RF              int        `json:"replication_factor"`
	K               int        `json:"semi_sync_acks"`
	MinDelayMs      int        `json:"min_delay_ms"`
	MaxDelayMs      int        `json:"max_delay_ms"`
}

func NewClusterUpdate(nodes []NodeInfo, leaderID, replicationMode string, rf, k, minDelayMs, maxDelayMs int) ClusterUpdate {
	return ClusterUpdate{
		Type:            TypeClusterUpdate,
		Nodes:           nodes,
		LeaderID:        leaderID,
		ReplicationMode: replicationMode,
		RF:              rf,
		K:               k,
		MinDelayMs:      minDelayMs,
		MaxDelayMs:      maxDelayMs,
	}
}

func (clusterUpdate *ClusterUpdate) Validate() error {
	if _, ok := allowedReplicationModes[clusterUpdate.ReplicationMode]; !ok {
		return NewBadRequestError("invalid replication mode: " + clusterUpdate.ReplicationMode)
	}

	if len(clusterUpdate.Nodes) == 0 {
		return NewBadRequestError("nodes list is empty")
	}

	if clusterUpdate.RF < 1 || clusterUpdate.RF > len(clusterUpdate.Nodes) {
		return NewBadRequestError("replication factor must be between 1 and number of nodes")
	}

	if (clusterUpdate.ReplicationMode == ReplicationSemiSync) && (clusterUpdate.K < 1 || clusterUpdate.K >= clusterUpdate.RF) {
		return NewBadRequestError("semi-sync acks must be between 1 and replication factor - 1")
	}

	if clusterUpdate.MinDelayMs < 0 || clusterUpdate.MaxDelayMs < 0 || clusterUpdate.MinDelayMs > clusterUpdate.MaxDelayMs {
		return NewBadRequestError("invalid delay range")
	}

	return nil
}

type BaseClientRequest struct {
	Type        string    `json:"type"`
	RequestUUID uuid.UUID `json:"request_id"`
	ClientUUID  uuid.UUID `json:"client_id"`
}

type ClientPutRequest struct {
	BaseClientRequest
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewClientPutRequest(requestUUID, clientUUID uuid.UUID, key, value string) ClientPutRequest {
	return ClientPutRequest{
		BaseClientRequest: BaseClientRequest{
			Type:        TypeClientPutRequest,
			RequestUUID: requestUUID,
			ClientUUID:  clientUUID,
		},
		Key:   key,
		Value: value,
	}
}

type ClientGetRequest struct {
	BaseClientRequest
	Key string `json:"key"`
}

func NewClientGetRequest(requestUUID, clientUUID uuid.UUID, key string) ClientGetRequest {
	return ClientGetRequest{
		BaseClientRequest: BaseClientRequest{
			Type:        TypeClientGetRequest,
			RequestUUID: requestUUID,
			ClientUUID:  clientUUID,
		},
		Key: key,
	}
}

type ClientDumpRequest struct {
	BaseClientRequest
}

func NewClientDumpRequest(requestUUID, clientUUID uuid.UUID) ClientDumpRequest {
	return ClientDumpRequest{
		BaseClientRequest: BaseClientRequest{
			Type:        TypeClientDumpRequest,
			RequestUUID: requestUUID,
			ClientUUID:  clientUUID,
		},
	}
}

type BaseClientResponse struct {
	Type        string    `json:"type"`
	RequestUUID uuid.UUID `json:"request_id"`
	Node        NodeInfo  `json:"node"`
	Status      string    `json:"status"`
	ErrorCode   string    `json:"error_code,omitempty"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
}

func NewClientResponse(
	requestUUID uuid.UUID,
	node NodeInfo,
	err error,
) BaseClientResponse {
	if err == nil {
		return BaseClientResponse{
			RequestUUID: requestUUID,
			Node:        node,
			Status:      StatusOK,
		}
	}

	var ce ClientError
	if errors.As(err, &ce) {
		return BaseClientResponse{
			RequestUUID: requestUUID,
			Node:        node,
			Status:      StatusError,
			ErrorCode:   ce.ErrorCode(),
			ErrorMsg:    ce.Error(),
		}
	}

	return BaseClientResponse{
		RequestUUID: requestUUID,
		Node:        node,
		Status:      StatusError,
		ErrorCode:   ErrorBadRequest,
		ErrorMsg:    err.Error(),
	}
}

type ClientPutResponse struct {
	BaseClientResponse
}

func NewClientPutResponse(requestUUID uuid.UUID, node NodeInfo, error error) ClientPutResponse {

	base := NewClientResponse(requestUUID, node, error)
	base.Type = TypeClientPutResponse

	return ClientPutResponse{
		BaseClientResponse: base,
	}
}

type ClientGetResponse struct {
	BaseClientResponse
	Value string `json:"value,omitempty"`
}

func NewClientGetResponse(requestUUID uuid.UUID, node NodeInfo, value string, error error) ClientGetResponse {
	base := NewClientResponse(requestUUID, node, error)
	base.Type = TypeClientGetResponse

	return ClientGetResponse{
		BaseClientResponse: base,
		Value:              value,
	}
}

type ClientDumpResponse struct {
	BaseClientResponse
	Dump map[string]string `json:"dump"`
}

func NewClientDumpResponse(requestUUID uuid.UUID, node NodeInfo, dump map[string]string, error error) ClientDumpResponse {
	base := NewClientResponse(requestUUID, node, error)
	base.Type = TypeClientDumpResponse

	return ClientDumpResponse{
		BaseClientResponse: base,
		Dump:               dump,
	}
}

type ReplicationPut struct {
	Type        string    `json:"type"`
	OperationID uuid.UUID `json:"operation_id"`
	Leader      NodeInfo  `json:"leader"`

	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewReplicationPut(operationID uuid.UUID, leader NodeInfo, key, value string) ReplicationPut {
	return ReplicationPut{
		Type:        TypeReplicationPut,
		OperationID: operationID,
		Leader:      leader,
		Key:         key,
		Value:       value,
	}
}

type ReplicationAck struct {
	Type        string    `json:"type"`
	OperationID uuid.UUID `json:"operation_id"`
	Node        NodeInfo  `json:"node"`
}

func NewReplicationAck(operationID uuid.UUID, node NodeInfo) ReplicationAck {
	return ReplicationAck{
		Type:        TypeReplicationAck,
		OperationID: operationID,
		Node:        node,
	}
}
