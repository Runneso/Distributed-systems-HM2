package main

import (
	""
	"HM2/internal/protocol"
	"encoding/json"
	"fmt"
)

func main() {
	cu := protocol.ClusterUpdate{}
	s := `{"type":"CLUSTER_UPDATE","nodes":[{"node_id":"node-1","hostname":"127.0.0.1","port":9001},{"node_id":"node-2","hostname":"127.0.0.1","port":9002},{"node_id":"node-3","hostname":"127.0.0.1","port":9003}],"leader_id":"123","replication_mode":"semi-sync","replication_factor":1,"quorum_size":50,"min_delay_ms":200,"max_delay_ms":250}`
	data := []byte(s)
	json.Unmarshal(data, &cu)
	fmt.Println(cu)
}
