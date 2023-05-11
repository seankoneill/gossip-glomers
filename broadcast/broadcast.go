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

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any

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
        type TopologyBody struct {
            Msg_id int
            Type string
            Topology map[string][]string
        }
        var body TopologyBody

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        topology = body.Topology[msg.Dest]
        reply := map[string]string{"type": "topology_ok"}
        return n.Reply(msg, reply)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
