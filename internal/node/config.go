package node

import "HM2/internal/protocol"

type ClusterConfig struct {
	leaderID        string
	replicationMode string
	rf              int
	k               int
	delayMinMs      int
	delayMaxMs      int
	tryingForever   bool
}

func NewClusterConfig(leaderID, replicationMode string, rf, k, delayMinMs, delayMaxMs int, tryingForever bool) *ClusterConfig {
	return &ClusterConfig{
		leaderID:        leaderID,
		replicationMode: replicationMode,
		rf:              rf,
		k:               k,
		delayMinMs:      delayMinMs,
		delayMaxMs:      delayMaxMs,
		tryingForever:   tryingForever,
	}
}

func (config *ClusterConfig) Update(request *protocol.ClusterUpdateRequest) {
	config.leaderID = request.LeaderID
	config.replicationMode = request.ReplicationMode
	config.rf = request.RF
	config.k = request.K
	config.delayMinMs = request.MinDelayMs
	config.delayMaxMs = request.MaxDelayMs
}
