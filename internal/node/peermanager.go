package node

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"HM2/internal/protocol"
	"HM2/pkg/random"

	"github.com/google/uuid"
)

const (
	DefaultLeaderID          = "A"
	DefaultReplicationMode   = protocol.ReplicationSync
	DefaultReplicationFactor = 1
	DefaultAckNeed           = 1
	DefaultMinDelayMs        = 0
	DefaultMaxDelayMs        = 0
	DefaultTryingForever     = false
)

type ClusterRelease struct {
	needAcks  int
	totalAcks int
	acked     map[string]struct{}
	syncDone  chan struct{}
	cancel    context.CancelFunc
}

type PeerManager struct {
	mu sync.RWMutex

	self   protocol.NodeInfo
	nodes  map[string]protocol.NodeInfo
	config *ClusterConfig

	releases map[uuid.UUID]*ClusterRelease
	peers    map[string]*PeerConn
}

func NewPeerManager(self protocol.NodeInfo) *PeerManager {
	return &PeerManager{
		self:     self,
		nodes:    make(map[string]protocol.NodeInfo),
		peers:    make(map[string]*PeerConn),
		releases: make(map[uuid.UUID]*ClusterRelease),
		config: NewClusterConfig(
			DefaultLeaderID,
			DefaultReplicationMode,
			DefaultReplicationFactor,
			DefaultAckNeed,
			DefaultMinDelayMs,
			DefaultMaxDelayMs,
			DefaultTryingForever,
		),
	}
}

func (peerManager *PeerManager) IsLeader() bool {
	peerManager.mu.RLock()
	defer peerManager.mu.RUnlock()
	return peerManager.self.ID == peerManager.config.leaderID
}

func (peerManager *PeerManager) GetLeaderID() string {
	peerManager.mu.RLock()
	defer peerManager.mu.RUnlock()
	return peerManager.config.leaderID
}

func (peerManager *PeerManager) ApplyClusterUpdate(request *protocol.ClusterUpdateRequest) {
	peerManager.mu.Lock()
	defer peerManager.mu.Unlock()
	peerManager.config.Update(request)

	peerManager.nodes = make(map[string]protocol.NodeInfo)
	for _, node := range request.Nodes {
		peerManager.nodes[node.ID] = node
	}

	isLeader := peerManager.self.ID == peerManager.config.leaderID

	for id, connection := range peerManager.peers {
		connection.Close()
		delete(peerManager.peers, id)
	}

	if isLeader {
		for id, node := range peerManager.nodes {
			if id == peerManager.self.ID {
				continue
			}
			addr := net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
			connection := NewPeerConn(id, addr, peerManager.onAck)
			peerManager.startReconnectLoop(connection)
			peerManager.peers[id] = connection
			go func(p *PeerConn) {
				_ = p.Connect()
			}(connection)
		}
	}
}

func (peerManager *PeerManager) Release(request *protocol.ClientPutRequest) error {
	peerManager.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())

	operationID := request.RequestUUID

	release := &ClusterRelease{
		needAcks:  peerManager.clcNeedAcks(),
		totalAcks: peerManager.config.rf - 1,
		acked:     make(map[string]struct{}),
		syncDone:  make(chan struct{}),
		cancel:    cancel,
	}
	peerManager.releases[operationID] = release

	replicationRequest := protocol.NewReplicationPut(
		operationID,
		peerManager.nodes[peerManager.config.leaderID],
		request.Key,
		request.Value,
	)

	if release.needAcks == 0 {
		close(release.syncDone)
	}

	for _, connection := range peerManager.peers {
		go peerManager.retrying(
			ctx,
			connection,
			replicationRequest,
			peerManager.config.delayMinMs,
			peerManager.config.delayMaxMs,
		)
	}

	tryingForever := peerManager.config.tryingForever

	peerManager.mu.Unlock()

	select {
	case <-release.syncDone:
		return nil
	case <-time.After(5 * time.Second):
		if !tryingForever {
			cancel()
		}
		return protocol.NewTimeoutError("timeout waiting for release to complete")
	}

}

func (peerManager *PeerManager) clcNeedAcks() int {
	if peerManager.config.replicationMode == protocol.ReplicationAsync {
		return 0
	}
	if peerManager.config.replicationMode == protocol.ReplicationSync {
		return peerManager.config.rf - 1
	}
	return peerManager.config.k
}

func (peerManager *PeerManager) retrying(
	ctx context.Context,
	connection *PeerConn,
	request protocol.ReplicationPut,
	delayMinMs, delayMaxMs int,
) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(1000+delayMaxMs)) // avoid random choose in select
	defer ticker.Stop()
	peerManager.noiseSleep(delayMinMs, delayMaxMs)
	_ = connection.Send(request)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peerManager.noiseSleep(delayMinMs, delayMaxMs)
			_ = connection.Send(request)
		}
	}
}

func (peerManager *PeerManager) onAck(request *protocol.ReplicationAck) {
	peerManager.mu.Lock()
	defer peerManager.mu.Unlock()
	release, ok := peerManager.releases[request.OperationID]
	if !ok {
		return
	}
	release.acked[request.Node.ID] = struct{}{}

	if len(release.acked) >= release.needAcks {
		select {
		case <-release.syncDone:
		default:
			close(release.syncDone)
		}
	}

	if len(release.acked) >= release.totalAcks {
		release.cancel()
		delete(peerManager.releases, request.OperationID)
	}
}

func (peerManager *PeerManager) noiseSleep(delayMinMs, delayMaxMs int) {
	noise := random.RandInt(delayMinMs, delayMaxMs)
	duration := time.Duration(noise) * time.Millisecond
	time.Sleep(duration)
}

func (peerManager *PeerManager) startReconnectLoop(connection *PeerConn) {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-connection.done:
				return
			default:
			}
			connection.mu.Lock()
			alive := connection.conn != nil
			connection.mu.Unlock()
			if alive {
				continue
			}
			_ = connection.Connect()
		}
	}()
}
