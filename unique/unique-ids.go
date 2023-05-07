package main

import (
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    counter := 1

    n.Handle("echo", func(msg maelstrom.Message) error {
        var body map[string]any

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "echo_ok"

        return n.Reply(msg, body)
    })

    n.Handle("generate", func(msg maelstrom.Message) error {
        var body map[string]any

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "generate_ok"
        body["id"] = msg.Dest + strconv.Itoa(counter)
        counter++

        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

func gen_id() {

}
