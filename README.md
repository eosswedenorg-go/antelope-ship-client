# Eos SHIP Client

Client Implementation of eosio state-history websocket.

### Install package

``` bash
go get -u github.com/eosswedenorg-go/eos-ship-client@latest
```

### Example

```go
package main

import (
    "os"
    "os/signal"
    "log"
    "time"
    "github.com/eoscanada/eos-go/ship"
    shipclient "github.com/eosswedenorg-go/eos-ship-client"
)

func processBlock(block *ship.GetBlocksResultV0) {

    if block.ThisBlock.BlockNum % 100 == 0 {
        log.Printf("Current: %d, Head: %d\n", block.ThisBlock.BlockNum, block.Head.BlockNum)
    }
}

func processTraces(traces []*ship.TransactionTraceV0) {

    for _, trace := range traces {
        log.Println("Trace ID:", trace.ID)
    }
}

func main() {

    // Create done and interrupt channels.
    done := make(chan bool)
    interrupt := make(chan os.Signal, 1)

    // Register interrupt channel to receive interrupt messages
    signal.Notify(interrupt, os.Interrupt)

    client := shipclient.NewClient(0, shipclient.NULL_BLOCK_NUMBER, false)
    client.BlockHandler = processBlock
    client.TraceHandler = processTraces

    err := client.Connect("127.0.0.1:8089")
    if err != nil {
        log.Println(err)
        return
    }

    err = client.SendBlocksRequest()
    if err != nil {
        log.Println(err)
        return
    }

    // Spawn message read loop in another thread.
    go func() {
        for {
            err := client.Read()
            if err != nil {
                log.Print(err.Error())

                // Bail out on socket read error.
                if err.Type == shipclient.ErrSockRead {
                    break
                }
            }
        }

        client.Close()

        // Client exited. signal that we are done.
        done <- true
    }()

    // Enter event loop in main thread
    for {
        select {
        case <-interrupt:
            log.Println("Interrupt, closing")

            // Cleanly close the connection by sending a close message and then
            // waiting (with timeout) for the server to close the connection.
            client.SendCloseMessage()

            select {
                case <-done: log.Println("Closed")
                case <-time.After(time.Second * 4): log.Println("Timeout");
            }
            return
        case <-done:
            log.Println("Closed")
            return
        }
    }
}
```

### Author

Henrik Hautakoski - [Sw/eden](https://eossweden.org/) - [henrik@eossweden.org](mailto:henrik@eossweden.org)
