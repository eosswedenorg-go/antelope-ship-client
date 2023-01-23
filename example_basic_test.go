package antelope_ship_client_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

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
var APIURL string // = "http://127.0.0.1:8088"

// If `APIURL` is not set, this is the block
// where we request ship to start sending blocks from.
// This should be something other than zero. check a block explorer or /v1/chain/get_info for the latest block number.
// or use APIURL to make the code fetch it itself.
var startBlock uint32 = 0

// True if the client should request a status message from the ship server on startup.
var sendStatus bool = true

// True if traces should be printed (this can get spammy)
var printTraces bool = false

func initHandler(abi *eos.ABI) {
	log.Println("Server abi:", abi.Version)
}

func processBlock(block *ship.GetBlocksResultV0) {
	if block.ThisBlock.BlockNum%100 == 0 {
		log.Printf("Current: %d, Head: %d\n", block.ThisBlock.BlockNum, block.Head.BlockNum)
	}
}

func processTraces(traces []*ship.TransactionTraceV0) {
	for _, trace := range traces {
		log.Println("Trace ID:", trace.ID)
	}
}

func processStatus(status *ship.GetStatusResultV0) {
	log.Println("-- Status --")
	log.Println("Head", status.Head.BlockNum, status.Head.BlockID)
	log.Println("ChainStateBeginBlock", status.ChainStateBeginBlock, "ChainStateEndBlock", status.ChainStateEndBlock)
}

func Example_basic() {
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
			log.Println("Failed to get info:", err)
			return
		}
	}

	log.Println("Connecting to ship starting at block:", startBlock)

	client := shipclient.NewClient(shipclient.WithStartBlock(startBlock))
	client.InitHandler = initHandler
	client.BlockHandler = processBlock
	client.StatusHandler = processStatus

	// Only assign trace handler if printTraces is true.
	if printTraces {
		client.TraceHandler = processTraces
	}

	// Connect to SHIP client
	err := client.Connect(shipHost)
	if err != nil {
		log.Println(err)
		return
	}

	// Request streaming of blocks from ship
	err = client.SendBlocksRequest()
	if err != nil {
		log.Println(err)
		return
	}

	// Request status message from ship
	if sendStatus {
		err = client.SendStatusRequest()
		if err != nil {
			log.Println(err)
			return
		}
	}

	// Spawn message read loop in another thread.
	go func() {
		for {
			err := client.Read()
			if err != nil {
				log.Print(err)

				if e, ok := err.(shipclient.ShipClientError); ok {
					if e.Type == shipclient.ErrSockRead {
						break
					}
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
			err := client.Shutdown()
			if err != nil {
				log.Println("Failed to send close message", err)
			}

			select {
			case <-done:
				log.Println("Closed")
			case <-time.After(time.Second * 4):
				log.Println("Timeout")
			}
			return
		case <-done:
			log.Println("Closed")
			return
		}
	}
}
