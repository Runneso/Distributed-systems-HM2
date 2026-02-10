package node

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"HM2/internal/protocol"
)

const (
	DefaultBufferSize = 1024
)

type PeerConn struct {
	peerID string
	addr   string

	mu           sync.Mutex
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	loopsStarted bool

	sendCh chan any
	done   chan struct{}

	onAck func(*protocol.ReplicationAck)
}

func NewPeerConn(peerID, addr string, onAck func(*protocol.ReplicationAck)) *PeerConn {
	return &PeerConn{
		peerID: peerID,
		addr:   addr,
		sendCh: make(chan any, DefaultBufferSize),
		done:   make(chan struct{}),
		onAck:  onAck,
	}
}

func (connection *PeerConn) Connect() error {
	select {
	case <-connection.done:
		return io.ErrClosedPipe
	default:
	}

	conn, err := net.Dial("tcp", connection.addr)
	if err != nil {
		return err
	}

	connection.mu.Lock()
	if connection.conn != nil {
		_ = conn.Close()
		connection.mu.Unlock()
		return nil
	}

	connection.conn = conn
	connection.reader = bufio.NewReader(conn)
	connection.writer = bufio.NewWriter(conn)

	startLoops := !connection.loopsStarted
	if startLoops {
		connection.loopsStarted = true
	}
	connection.mu.Unlock()

	if startLoops {
		go connection.writerLoop()
		go connection.readerLoop()
	}
	return nil
}

func (connection *PeerConn) Close() {
	select {
	case <-connection.done:
		return
	default:
		close(connection.done)
	}
	connection.dropConn()
}

func (connection *PeerConn) Send(msg any) error {
	select {
	case connection.sendCh <- msg:
		return nil
	case <-connection.done:
		return io.ErrClosedPipe
	}
}

func (connection *PeerConn) writerLoop() {
	for {
		select {
		case msg := <-connection.sendCh:
			connection.mu.Lock()

			if connection.writer == nil {
				connection.mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				continue
			}
			enc := json.NewEncoder(connection.writer)
			err := enc.Encode(msg)
			if err == nil {
				err = connection.writer.Flush()
			}
			connection.mu.Unlock()

			if err != nil {
				connection.dropConn()
				continue
			}

		case <-connection.done:
			return
		}
	}
}

func (connection *PeerConn) readerLoop() {
	for {
		select {
		case <-connection.done:
			return
		default:
		}

		connection.mu.Lock()
		r := connection.reader
		connection.mu.Unlock()

		if r == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		line, err := r.ReadBytes('\n')
		if err != nil {
			connection.dropConn()
			continue
		}

		var env struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(line, &env) != nil {
			continue
		}

		if env.Type == protocol.TypeReplicationAck {
			var ack protocol.ReplicationAck
			if json.Unmarshal(line, &ack) == nil && connection.onAck != nil {
				slog.Info("node handle request", "type", env.Type, "operation_id", ack.OperationID)
				connection.onAck(&ack)
			}
		}
	}
}

func (connection *PeerConn) dropConn() {
	connection.mu.Lock()
	if connection.conn != nil {
		_ = connection.conn.Close()
	}
	connection.conn = nil
	connection.reader = nil
	connection.writer = nil
	connection.mu.Unlock()
}
