package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    var msg_set map[int]bool = make(map[int]bool)
    var topology []string
    inited := false

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any

        if !inited {
            topology = n.NodeIDs()
            for i,id := range topology {
                if id == n.ID() {
                    topology = append(topology[:i], topology[i+1:] ...)
                    inited = true
                    break
                }
            }
        }

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        value := int(body["message"].(float64)); 

        if !msg_set[value] {
            msg_set[value] = true
            for _,neighbour := range topology {
                n.Send(neighbour,body)
            }
        }

        body["type"] = "broadcast_ok"
        delete(body,"message")

        return n.Reply(msg, body)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "read_ok"

        keys := []int{}
        for key := range msg_set {
            keys = append(keys,key)
        }
        body["messages"] = keys

        return n.Reply(msg, body)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        if topology_map, ok := body["topology"].(map[string]any); ok {
            topology_map = map[string]any(topology_map)
            if topology_list, t_ok := topology_map[msg.Dest].([]string); t_ok {
                topology = []string(topology_list)
            }
        }

        body["type"] = "topology_ok"
        delete(body,"topology")

        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
