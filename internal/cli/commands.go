package cli

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"HM2/internal/protocol"

	"github.com/google/uuid"
)

type Command func(st *State, args []string, out io.Writer) error

func Commands() map[string]Command {
	return map[string]Command{
		"help":                  cmdHelp,
		"addNode":               cmdAddNode,
		"removeNode":            cmdRemoveNode,
		"listNodes":             cmdListNodes,
		"setLeader":             cmdSetLeader,
		"setReplication":        cmdSetReplication,
		"setRF":                 cmdSetRF,
		"setSemiSyncAcks":       cmdSetK,
		"setReplicationDelayMs": cmdSetDelay,

		"put":  cmdPut,
		"get":  cmdGet,
		"dump": cmdDump,
	}
}

func ExecuteLine(st *State, line string, out io.Writer) error {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	name := fields[0]
	args := fields[1:]

	cmd, ok := Commands()[name]
	if !ok {
		return fmt.Errorf("unknown command: %s (try 'help')", name)
	}
	return cmd(st, args, out)
}

func cmdHelp(_ *State, _ []string, out io.Writer) error {
	_, _ = fmt.Fprintln(out, "Cluster commands:")
	_, _ = fmt.Fprintln(out, "  addNode <nodeId> <host> <port>")
	_, _ = fmt.Fprintln(out, "  removeNode <nodeId>")
	_, _ = fmt.Fprintln(out, "  listNodes")
	_, _ = fmt.Fprintln(out, "  setLeader <nodeId>")
	_, _ = fmt.Fprintln(out, "  setReplication async|semi-sync|sync")
	_, _ = fmt.Fprintln(out, "  setRF <int> (1..N)")
	_, _ = fmt.Fprintln(out, "  setSemiSyncAcks <int> (K; only for semi-sync)")
	_, _ = fmt.Fprintln(out, "  setReplicationDelayMs <min> <max>")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "User commands:")
	_, _ = fmt.Fprintln(out, "  put <key> <value> [--target <nodeId>] [--client <clientId>]")
	_, _ = fmt.Fprintln(out, "  get <key> [--target <nodeId>] [--client <clientId>]")
	_, _ = fmt.Fprintln(out, "  dump [--target <nodeId>]")
	return nil
}

func pickAnyNodeID(st *State) (string, bool) {
	if len(st.nodes) == 0 {
		return "", false
	}
	ids := make([]string, 0, len(st.nodes))
	for id := range st.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[0], true
}

func randomTargetID(st *State) (string, bool) {
	if len(st.nodes) == 0 {
		return "", false
	}
	ids := make([]string, 0, len(st.nodes))
	for id := range st.nodes {
		ids = append(ids, id)
	}
	return ids[rand.Intn(len(ids))], true
}

func validateStateConfig(st *State) error {
	clusterSize := len(st.nodes)
	if clusterSize == 0 {
		return errors.New("cluster is empty")
	}

	if st.leaderID == "" {
		return errors.New("leader is not set")
	}
	if _, ok := st.nodes[st.leaderID]; !ok {
		return fmt.Errorf("leader %s not in nodes", st.leaderID)
	}

	switch st.replicationMode {
	case protocol.ReplicationAsync, protocol.ReplicationSemiSync, protocol.ReplicationSync:
	default:
		return fmt.Errorf("invalid replication mode: %s", st.replicationMode)
	}

	if st.rf < 1 {
		return fmt.Errorf("rf must be >= 1, got %d", st.rf)
	}
	if st.rf > clusterSize {
		return fmt.Errorf("rf (%d) must be <= clusterSize (%d)", st.rf, clusterSize)
	}

	if st.delayMinMs < 0 || st.delayMaxMs < 0 || st.delayMinMs > st.delayMaxMs {
		return fmt.Errorf("invalid delay range: %d..%d", st.delayMinMs, st.delayMaxMs)
	}

	if st.replicationMode == protocol.ReplicationSemiSync {
		if st.k < 1 || st.k > st.rf-1 {
			return fmt.Errorf("semi-sync: K must be in [1..RF-1], got K=%d RF=%d", st.k, st.rf)
		}
	}

	return nil
}

func broadcastClusterUpdate(st *State, out io.Writer) error {
	req := st.AsClusterUpdate()

	if err := req.Validate(); err != nil {
		return err
	}

	ok := 0
	total := 0

	for id, n := range st.nodes {
		total++
		addr := addrOfNode(n)
		var resp protocol.ClusterUpdateResponse
		err := sendJSONLine(addr, req, &resp)
		if err != nil {
			_, _ = fmt.Fprintf(out, "node %s (%s): %v\n", id, addr, err)
			continue
		}
		if resp.Status == protocol.StatusError {
			_, _ = fmt.Fprintf(out, "node %s (%s): rejected update: %s (%s)\n", id, addr, resp.ErrorMsg, resp.ErrorCode)
			continue
		}
		ok++
	}
	_, _ = fmt.Fprintf(out, "cluster_update delivered: %d/%d\n", ok, total)
	return nil
}

func parseTargetClient(st *State, args []string) (target string, client uuid.UUID, rest []string, err error) {
	target = ""
	client = uuid.Nil

	rest = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--target":
			if i+1 >= len(args) {
				return "", uuid.Nil, nil, errors.New("missing --target value")
			}
			target = args[i+1]
			i++
		case "--client":
			if i+1 >= len(args) {
				return "", uuid.Nil, nil, errors.New("missing --client value")
			}
			cid, e := uuid.Parse(args[i+1])
			if e != nil {
				return "", uuid.Nil, nil, fmt.Errorf("bad --client uuid: %w", e)
			}
			client = cid
			i++
		default:
			rest = append(rest, args[i])
		}
	}

	if client == uuid.Nil {
		client = uuid.New()
	}

	if target == "" {
		id, ok := randomTargetID(st)
		if !ok {
			return "", uuid.Nil, nil, errors.New("cluster is empty: no nodes for target")
		}
		target = id
	}

	return target, client, rest, nil
}

func printClientBase(out io.Writer, base protocol.BaseClientResponse) {
	_, _ = fmt.Fprintf(out, "status=%s node=%s request_id=%s",
		base.Status, base.Node.ID, base.RequestUUID)
	if base.ErrorCode != "" || base.ErrorMsg != "" {
		_, _ = fmt.Fprintf(out, " error_code=%s error_msg=%s", base.ErrorCode, base.ErrorMsg)
	}
	_, _ = fmt.Fprintln(out)
}

func cmdAddNode(st *State, args []string, out io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: addNode <nodeId> <host> <port>")
	}
	id := args[0]
	host := args[1]
	port, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("bad port: %w", err)
	}

	st.AddNode(id, host, port)

	if st.leaderID == "" {
		st.leaderID = id
	}
	if st.replicationMode == "" {
		st.replicationMode = protocol.ReplicationSync
	}
	if st.rf == 0 {
		st.rf = 1
	}
	if st.k == 0 {
		st.k = 1
	}

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdRemoveNode(st *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: removeNode <nodeId>")
	}
	id := args[0]
	st.RemoveNode(id)

	if st.leaderID == id {
		if newID, ok := pickAnyNodeID(st); ok {
			st.leaderID = newID
		} else {
			st.leaderID = ""
		}
	}

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdListNodes(st *State, _ []string, out io.Writer) error {
	ids := make([]string, 0, len(st.nodes))
	for id := range st.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	_, _ = fmt.Fprintf(out, "leader=%s replication=%s rf=%d k=%d delay=[%d..%d]ms\n",
		st.leaderID, st.replicationMode, st.rf, st.k, st.delayMinMs, st.delayMaxMs)

	for _, id := range ids {
		n := st.nodes[id]
		mark := ""
		if id == st.leaderID {
			mark = " (leader)"
		}
		_, _ = fmt.Fprintf(out, "- %s %s:%d%s\n", n.ID, n.Hostname, n.Port, mark)
	}
	return nil
}

func cmdSetLeader(st *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setLeader <nodeId>")
	}
	id := args[0]
	if _, ok := st.nodes[id]; !ok {
		return fmt.Errorf("unknown node: %s", id)
	}
	st.SetLeader(id)

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdSetReplication(st *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setReplication async|semi-sync|sync")
	}
	st.SetReplicationMode(args[0])

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdSetRF(st *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setRF <int>")
	}
	rf, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("bad rf: %w", err)
	}
	st.SetRF(rf)

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdSetK(st *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setSemiSyncAcks <int>")
	}
	k, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("bad k: %w", err)
	}
	st.SetK(k)

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdSetDelay(st *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: setReplicationDelayMs <min> <max>")
	}
	minV, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("bad min: %w", err)
	}
	maxV, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("bad max: %w", err)
	}
	st.SetDelayMs(minV, maxV)

	if err := validateStateConfig(st); err != nil {
		return err
	}
	if err := broadcastClusterUpdate(st, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdPut(st *State, args []string, out io.Writer) error {
	if len(args) < 2 {
		return errors.New("usage: put <key> <value> [--target <nodeId>] [--client <clientId>]")
	}

	key := args[0]
	val := args[1]

	targetID, clientID, _, err := parseTargetClient(st, args[2:])
	if err != nil {
		return err
	}

	n, ok := st.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	req := protocol.NewClientPutRequest(uuid.New(), clientID, key, val)

	var resp protocol.ClientPutResponse
	if err := sendJSONLine(addrOfNode(n), req, &resp); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, resp.BaseClientResponse)
	return nil
}

func cmdGet(st *State, args []string, out io.Writer) error {
	if len(args) < 1 {
		return errors.New("usage: get <key> [--target <nodeId>] [--client <clientId>]")
	}
	key := args[0]

	targetID, clientID, _, err := parseTargetClient(st, args[1:])
	if err != nil {
		return err
	}

	n, ok := st.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	req := protocol.NewClientGetRequest(uuid.New(), clientID, key)

	var resp protocol.ClientGetResponse
	if err := sendJSONLine(addrOfNode(n), req, &resp); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, resp.BaseClientResponse)
	_, _ = fmt.Fprintf(out, "found=%v value=%q\n", resp.Found, resp.Value)
	return nil
}

func cmdDump(st *State, args []string, out io.Writer) error {
	targetID := ""

	if len(args) >= 2 && args[0] == "--target" {
		targetID = args[1]
		args = args[2:]
	}
	if len(args) != 0 {
		return errors.New("usage: dump [--target <nodeId>]")
	}

	if targetID == "" {
		id, ok := randomTargetID(st)
		if !ok {
			return errors.New("cluster is empty: no nodes for target")
		}
		targetID = id
	}

	n, ok := st.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	req := protocol.NewClientDumpRequest(uuid.New(), uuid.New())

	var resp protocol.ClientDumpResponse
	if err := sendJSONLine(addrOfNode(n), req, &resp); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, resp.BaseClientResponse)
	_, _ = fmt.Fprintf(out, "dump=%v\n", resp.Dump)
	return nil
}
