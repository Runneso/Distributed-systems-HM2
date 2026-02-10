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

	TypeClusterUpdateRequest  = "CLUSTER_UPDATE_REQUEST"
	TypeClusterUpdateResponse = "CLUSTER_UPDATE_RESPONSE"
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

type ApplicationError interface {
	error
	ErrorCode() string
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

type ClusterUpdateRequest struct {
	RequestID       uuid.UUID  `json:"request_id"`
	Type            string     `json:"type"`
	Nodes           []NodeInfo `json:"nodes"`
	LeaderID        string     `json:"leader_id"`
	ReplicationMode string     `json:"replication_mode"`
	RF              int        `json:"replication_factor"`
	K               int        `json:"semi_sync_acks"`
	MinDelayMs      int        `json:"min_delay_ms"`
	MaxDelayMs      int        `json:"max_delay_ms"`
}

func NewClusterUpdateRequest(requestId uuid.UUID, nodes []NodeInfo, leaderID, replicationMode string, rf, k, minDelayMs, maxDelayMs int) ClusterUpdateRequest {
	return ClusterUpdateRequest{
		RequestID:       requestId,
		Type:            TypeClusterUpdateRequest,
		Nodes:           nodes,
		LeaderID:        leaderID,
		ReplicationMode: replicationMode,
		RF:              rf,
		K:               k,
		MinDelayMs:      minDelayMs,
		MaxDelayMs:      maxDelayMs,
	}
}

func (clusterUpdateRequest *ClusterUpdateRequest) Validate() error {
	if _, ok := allowedReplicationModes[clusterUpdateRequest.ReplicationMode]; !ok {
		return NewBadRequestError("invalid replication mode: " + clusterUpdateRequest.ReplicationMode)
	}

	if len(clusterUpdateRequest.Nodes) == 0 {
		return NewBadRequestError("nodes list is empty")
	}

	if clusterUpdateRequest.RF < 1 || clusterUpdateRequest.RF > len(clusterUpdateRequest.Nodes) {
		return NewNotEnoughReplicasError("replication factor must be between 1 and number of nodes")
	}

	if (clusterUpdateRequest.ReplicationMode == ReplicationSemiSync) && (clusterUpdateRequest.K < 1 || clusterUpdateRequest.K >= clusterUpdateRequest.RF) {
		return NewBadRequestError("semi-sync acks must be between 1 and replication factor - 1")
	}

	if clusterUpdateRequest.MinDelayMs < 0 || clusterUpdateRequest.MaxDelayMs < 0 || clusterUpdateRequest.MinDelayMs > clusterUpdateRequest.MaxDelayMs {
		return NewBadRequestError("invalid delay range")
	}

	return nil
}

type ClusterUpdateResponse struct {
	RequestID uuid.UUID `json:"request_id"`
	Type      string    `json:"type"`
	Node      NodeInfo  `json:"node"`
	Status    string    `json:"status"`
	ErrorCode string    `json:"error_code,omitempty"`
	ErrorMsg  string    `json:"error_msg,omitempty"`
}

func NewClusterUpdateResponse(requestID uuid.UUID, node NodeInfo, error error) ClusterUpdateResponse {
	if error == nil {
		return ClusterUpdateResponse{
			RequestID: requestID,
			Type:      TypeClusterUpdateResponse,
			Node:      node,
			Status:    StatusOK,
		}
	}

	var ae ApplicationError
	if errors.As(error, &ae) {
		return ClusterUpdateResponse{
			RequestID: requestID,
			Type:      TypeClusterUpdateResponse,
			Node:      node,
			Status:    StatusError,
			ErrorMsg:  ae.Error(),
			ErrorCode: ae.ErrorCode(),
		}
	}

	return ClusterUpdateResponse{
		RequestID: requestID,
		Type:      TypeClusterUpdateResponse,
		Node:      node,
		Status:    StatusError,
		ErrorMsg:  error.Error(),
		ErrorCode: ErrorBadRequest,
	}
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

	var ae ApplicationError
	if errors.As(err, &ae) {
		return BaseClientResponse{
			RequestUUID: requestUUID,
			Node:        node,
			Status:      StatusError,
			ErrorCode:   ae.ErrorCode(),
			ErrorMsg:    ae.Error(),
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
	Found bool   `json:"found"`
}

func NewClientGetResponse(requestUUID uuid.UUID, node NodeInfo, value string, found bool, error error) ClientGetResponse {
	base := NewClientResponse(requestUUID, node, error)
	base.Type = TypeClientGetResponse

	return ClientGetResponse{
		BaseClientResponse: base,
		Value:              value,
		Found:              found,
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
