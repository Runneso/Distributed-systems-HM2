package cli

import (
	"HM2/internal/protocol"

	"github.com/google/uuid"
)

type State struct {
	leaderID        string
	replicationMode string
	rf              int
	k               int
	nodes           map[string]protocol.NodeInfo
	delayMinMs      int
	delayMaxMs      int
}

func NewState() *State {
	return &State{
		nodes: make(map[string]protocol.NodeInfo),
	}
}

func (storage *State) AddNode(id, host string, port int) {
	storage.nodes[id] = protocol.NodeInfo{
		ID:       id,
		Hostname: host,
		Port:     port,
	}
}

func (storage *State) RemoveNode(id string) {
	delete(storage.nodes, id)
}

func (storage *State) ListNodes() []protocol.NodeInfo {
	arr := make([]protocol.NodeInfo, 0, len(storage.nodes))
	for _, node := range storage.nodes {
		arr = append(arr, node)
	}
	return arr
}

func (storage *State) SetLeader(id string) {
	storage.leaderID = id
}

func (storage *State) SetReplicationMode(mode string) {
	storage.replicationMode = mode
}

func (storage *State) SetRF(rf int) {
	storage.rf = rf
}

func (storage *State) SetK(k int) {
	storage.k = k
}

func (storage *State) SetDelayMs(delayMinMs, delayMaxMs int) {
	storage.delayMinMs = delayMinMs
	storage.delayMaxMs = delayMaxMs
}

func (storage *State) AsClusterUpdate() protocol.ClusterUpdateRequest {
	return protocol.NewClusterUpdateRequest(
		uuid.New(),
		storage.ListNodes(),
		storage.leaderID,
		storage.replicationMode,
		storage.rf,
		storage.k,
		storage.delayMinMs,
		storage.delayMaxMs,
	)
}
