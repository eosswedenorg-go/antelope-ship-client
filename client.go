
package eos_ship_client

import (
    "net/url"
    ws "github.com/gorilla/websocket"
    eos "github.com/eoscanada/eos-go"
    "github.com/eoscanada/eos-go/ship"
)

const NULL_BLOCK_NUMBER uint32 = 0xffffffff

type ShipClient struct {

    // Socket connection
    sock *ws.Conn

    // Counter for how many non-ACKed messages we have received.
    unconfirmed uint32

    // Block to start receiving notifications on.
    StartBlock uint32

    // Block to end receiving notifications on.
    EndBlock uint32

    // if only irreversible blocks should be sent.
    IrreversibleOnly bool

    // Max number of non-ACKed messages that may be sent.
    MaxMessagesInFlight uint32

    // Callback functions
    BlockHandler func(*ship.GetBlocksResultV0)
    TraceHandler func([]*ship.TransactionTraceV0)
    StatusHandler func(*ship.GetStatusResultV0)
    CloseHandler func()
}

// Create a new client
func NewClient(startBlock uint32, endBlock uint32, irreversibleOnly bool) (*ShipClient) {
    return &ShipClient{
        sock: nil,
        unconfirmed: 0,
        StartBlock: startBlock,
        EndBlock: endBlock,
        IrreversibleOnly: irreversibleOnly,
        MaxMessagesInFlight: 10,
        StatusHandler: nil,
        BlockHandler: nil,
        CloseHandler: nil,
    }
}

// Connect the client to a ship node
func (c *ShipClient) Connect(host string) error {

   return c.ConnectURL(url.URL{Scheme: "ws", Host: host, Path: "/"})
}

// Connect the client to a ship node
func (c *ShipClient) ConnectURL(url url.URL) error {

    sock, _, err := ws.DefaultDialer.Dial(url.String(), nil)
    if err != nil {
        return err
    }

    c.sock = sock
    return nil
}

// Send a block request to the ship server.
// This tells the server to start sending blocks to the client.
func (c *ShipClient) SendBlocksRequest() (error) {

    // Encode the request.
    bytes, err := eos.MarshalBinary(ship.Request{
        BaseVariant: eos.BaseVariant{
            TypeID: ship.RequestVariant.TypeID("get_blocks_request_v0"),
            Impl:   &ship.GetBlocksRequestV0{
                StartBlockNum: c.StartBlock,
                EndBlockNum: c.EndBlock,
                MaxMessagesInFlight: c.MaxMessagesInFlight,
                IrreversibleOnly: c.IrreversibleOnly,
                FetchBlock: true,
                FetchTraces: c.TraceHandler != nil,
                FetchDeltas: false,
            },
        },
    })

    if err != nil {
        return err
    }

    // Send the request.
    return c.sock.WriteMessage(ws.BinaryMessage, bytes)
}

// Send a status request to the ship server.
// This tells the server to start sending status message to the client.
func (c *ShipClient) SendStatusRequest() (error) {

    // Encode the request.
    bytes, err := eos.MarshalBinary(ship.Request{
        BaseVariant: eos.BaseVariant{
            TypeID: ship.RequestVariant.TypeID("get_status_request_v0"),
            Impl:   &ship.GetStatusRequestV0{},
        },
    })

    if err != nil {
        return err
    }

    // Send the request.
    return c.sock.WriteMessage(ws.BinaryMessage, bytes)
}

// Read messages from the client.
func (c *ShipClient) Read() (*ShipClientError) {

    for {
        var msg ship.Result

        msg_type, data, err := c.sock.ReadMessage()
        if err != nil {
            return &ShipClientError{ErrSockRead, err.Error()}
        }

        // Check if we need to ack messages
        c.unconfirmed += 1;
        if c.unconfirmed >= c.MaxMessagesInFlight {
            req := ship.NewGetBlocksAck(c.unconfirmed)
            err = c.sock.WriteMessage(ws.BinaryMessage, req)
            c.unconfirmed = 0
            if err != nil {
                return &ShipClientError{ErrACK, err.Error()}
            }
        }

        if msg_type != ws.BinaryMessage {
            return &ShipClientError{ErrParse, "Can only decode binary messages"}
        }

        // Unpack the message
        if err = eos.UnmarshalBinary(data, &msg); err != nil {
            return &ShipClientError{ErrParse, err.Error()}
        }

        // Parse message and route to correct callback.
        block, ok := msg.Impl.(*ship.GetBlocksResultV0)
        if ok {

            if block.ThisBlock == nil && block.Head != nil {
                continue
            }

            if c.BlockHandler != nil {
                c.BlockHandler(block)
            }

            if block.Traces != nil && c.TraceHandler != nil {
                c.TraceHandler(block.Traces.AsTransactionTracesV0())
            }

            if block.ThisBlock.BlockNum + 1 >= c.EndBlock {
                return c.SendCloseMessage()
            }

            break
        }

        status, ok := msg.Impl.(*ship.GetStatusResultV0)
        if ok && c.StatusHandler != nil {
            c.StatusHandler(status)
        }
        break
    }

    return nil
}

// Sends a close message to the server.
// indicating that the client wants to terminate the connection.
func (c *ShipClient) SendCloseMessage() (*ShipClientError) {

    if ! c.IsOpen() {
        return &ShipClientError{ErrNotConnected, "Socket not connected"}
    }

    err := c.sock.WriteMessage(ws.CloseMessage, ws.FormatCloseMessage(ws.CloseNormalClosure, ""))
    if err != nil {
        return &ShipClientError{ErrSendClose, err.Error()}
    }

    return nil
}

// Returns true if the websocket connection is open. false otherwise.
func (c *ShipClient) IsOpen() bool {
    return c.sock != nil
}

// Close the socket on the client side.
//
// NOTE: This method closes the underlying network connection without
// sending or waiting for a close message.
func (c *ShipClient) Close() (*ShipClientError) {

    if ! c.IsOpen() {
        return &ShipClientError{ErrNotConnected, "Socket not connected"}
    }

    c.sock.Close()
    c.sock = nil

    if c.CloseHandler != nil {
        c.CloseHandler()
    }

    return nil
}
