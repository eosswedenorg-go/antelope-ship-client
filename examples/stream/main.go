package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	eos "github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	shipclient "github.com/eosswedenorg-go/antelope-ship-client"
)

// -----------------------------
//  Config variables
// -----------------------------

// IP and port to the ship node.
var shipHost string = "ws://127.0.0.1:8089"

// Url to the antelope api on the same node as ship is running.
// Use this to fetch a sane value for `startBlock`
var APIURL string = "http://172.16.6.124:8688"

// If `APIURL` is not set, this is the block
// where we request ship to start sending blocks from.
// This should be something other than zero. check a block explorer or /v1/chain/get_info for the latest block number.
// or use APIURL to make the code fetch it itself.
var startBlock uint32 = 0

// True if the client should request a status message from the ship server on startup.
var sendStatus bool = true

// True if traces should be printed (this can get spammy)
var printTraces bool = false

// True if table deltas should be printed (this can get spammy)
var printTableDeltas bool = false

func initHandler(abi *eos.ABI) {
	log.Println("Server abi:", abi.Version)
}

func processBlock(block *ship.GetBlocksResultV0) {
	log.Printf("Block: %d %s\n",
		block.ThisBlock.BlockNum, block.ThisBlock.BlockID)
}

func processTableDeltas(deltas []*ship.TableDeltaV0) {
	for _, delta := range deltas {
		log.Println("Table Delta:", delta.Name, "rows:", len(delta.Rows))
	}
}

func processTraces(traces []*ship.TransactionTraceV0) {
	for _, trace := range traces {
		log.Println("Trace ID:", trace.ID)
	}
}

func processStatus(status *ship.GetStatusResultV0) {
	log.Println("-- Status START --")
	log.Println("Head", status.Head.BlockNum, status.Head.BlockID)
	log.Println("ChainStateBeginBlock", status.ChainStateBeginBlock, "ChainStateEndBlock", status.ChainStateEndBlock)
	log.Println("-- Status END --")
}

func main() {
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

	stream := shipclient.NewStream(shipclient.WithStartBlock(startBlock))
	stream.InitHandler = initHandler
	stream.BlockHandler = processBlock
	stream.StatusHandler = processStatus

	// Only assign trace handler if printTraces is true.
	if printTraces {
		stream.TraceHandler = processTraces
	}

	if printTableDeltas {
		stream.TableDeltaHandler = processTableDeltas
	}

	// Connect to SHIP client
	err := stream.Connect(shipHost)
	if err != nil {
		log.Fatalln(err)
	}

	// Request streaming of blocks from ship
	err = stream.SendBlocksRequest()
	if err != nil {
		log.Fatalln(err)
	}

	// Request status message from ship
	if sendStatus {
		err = stream.SendStatusRequest()
		if err != nil {
			log.Fatalln(err)
		}
	}

	// Spawn message read loop in another thread.
	go func() {
		// Create interrupt channels.
		interrupt := make(chan os.Signal, 1)

		// Register interrupt channel to receive interrupt messages
		signal.Notify(interrupt, os.Interrupt)

		// Enter event loop in main thread
		for {
			select {
			case <-interrupt:
				log.Println("Interrupt, closing")

				// Cleanly close the connection by sending a close message and then
				// waiting (with timeout) for the server to close the connection.
				err := stream.Shutdown()
				if err != nil {
					log.Println("Failed to close stream", err)
				}
				return
			}
		}
	}()

	err = stream.Run()
	log.Println(err)
}
