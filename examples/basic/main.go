package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	eos "github.com/eoscanada/eos-go"
	"github.com/eosswedenorg-go/antelope-ship-client/websocket"
	"github.com/pnx/antelope-go/ship"
)

// -----------------------------
//  Config variables
// -----------------------------

// IP and port to the ship node.
var shipHost string = "ws://127.0.0.1:8089"

// Url to the antelope api on the same node as ship is running.
// Use this to fetch a sane value for `startBlock`
var APIURL string // = "http://127.0.0.1:8088"

// If `APIURL` is not set, this is the block
// where we request ship to start sending blocks from.
// This should be something other than zero. check a block explorer or /v1/chain/get_info for the latest block number.
// or use APIURL to make the code fetch it itself.
var startBlock uint32 = 0

func main() {
	// Create done and interrupt channels.
	done := make(chan bool)
	interrupt := make(chan os.Signal, 1)

	// Register interrupt channel to receive interrupt messages
	signal.Notify(interrupt, os.Interrupt)

	// Get start block from chain info
	if APIURL != "" {
		chainInfo, err := eos.New(APIURL).GetInfo(context.Background())
		if err == nil {
			startBlock = chainInfo.HeadBlockNum
		} else {
			log.Fatalln("Failed to get info:", err)
		}
	}

	log.Println("Connecting to ship starting at block:", startBlock)

	client := websocket.NewClient()

	// Connect to SHIP client
	err := client.Connect(context.Background(), shipHost)
	if err != nil {
		log.Fatalln(err)
	}

	// Request streaming of blocks from ship
	err = client.Write(ship.Request{
		BlocksRequest: &ship.GetBlocksRequestV0{
			StartBlockNum:       startBlock,
			EndBlockNum:         0xffffffff,
			MaxMessagesInFlight: 0xffffffff,
			IrreversibleOnly:    false,
			FetchBlock:          true,
			FetchTraces:         false,
			FetchDeltas:         false,
			HavePositions:       []*ship.BlockPosition{},
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Spawn message read loop in another thread.
	go func() {
		for {
			msg, err := client.Read()
			if err != nil {
				log.Print(err)
				break
			}

			if msg.BlocksResult != nil {
				block := msg.BlocksResult
				log.Printf("Current: %d, Head: %d\n", block.ThisBlock.BlockNum, block.Head.BlockNum)
			} else if msg.StatusResult != nil {
				status := msg.StatusResult
				log.Printf("Status, Chain block: %d, Trace block: %d\n", status.ChainStateBeginBlock, status.TraceBeginBlock)
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
			defer cancel()

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			if err := client.Shutdown(ctx); err != nil {
				log.Println("Failed to close websocket", err)
			}

		case <-done:
			log.Println("Closed")
			return
		}
	}
}
