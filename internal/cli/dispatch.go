package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"HM2/internal/protocol"
)

const (
	defaultDialTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultReadTimeout  = 10 * time.Second
)

func sendJSONLine(addr string, req any, resp any) error {
	d := net.Dialer{Timeout: defaultDialTimeout}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
	w := bufio.NewWriter(conn)
	enc := json.NewEncoder(w)
	if err = enc.Encode(req); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err = w.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if resp == nil {
		return nil
	}

	_ = conn.SetReadDeadline(time.Now().Add(defaultReadTimeout))
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if err := json.Unmarshal(line, resp); err != nil {
		return fmt.Errorf("unmarshal resp: %w (raw=%q)", err, string(line))
	}
	return nil
}

func addrOfNode(n protocol.NodeInfo) string {
	return net.JoinHostPort(n.Hostname, fmt.Sprintf("%d", n.Port))
}
