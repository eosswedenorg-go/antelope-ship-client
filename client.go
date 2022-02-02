
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

func (this *ShipClient) Connect(host string) (error) {

    url := url.URL{Scheme: "ws", Host: host,  Path: "/"}

    sock, _, err := ws.DefaultDialer.Dial(url.String(), nil)
    if err != nil {
        return err
    }

    this.sock = sock
    return nil
}

func (this *ShipClient) SendBlocksRequest() (error) {

    // Encode the request.
    bytes, err := eos.MarshalBinary(ship.Request{
		BaseVariant: eos.BaseVariant{
			TypeID: ship.RequestVariant.TypeID("get_blocks_request_v0"),
			Impl:   &ship.GetBlocksRequestV0{
                StartBlockNum: this.StartBlock,
                EndBlockNum: this.EndBlock,
                MaxMessagesInFlight: this.MaxMessagesInFlight,
                IrreversibleOnly: this.IrreversibleOnly,
                FetchBlock: true,
                FetchTraces: this.TraceHandler != nil,
                FetchDeltas: false,
            },
		},
	})

	if err != nil {
        return err
	}

    // Send the request.
    return this.sock.WriteMessage(ws.BinaryMessage, bytes)
}

func (this *ShipClient) Read() (*ShipClientError) {

    for {
        var msg ship.Result

        msg_type, data, err := this.sock.ReadMessage()
        if err != nil {
            return &ShipClientError{ErrSockRead, err.Error()}
        }

        // Check if we need to ack messages
        this.unconfirmed += 1;
        if this.unconfirmed >= this.MaxMessagesInFlight {
            req := ship.NewGetBlocksAck(this.unconfirmed)
            err = this.sock.WriteMessage(ws.BinaryMessage, req)
            this.unconfirmed = 0
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

            if this.BlockHandler != nil {
                this.BlockHandler(block)
            }

            if block.Traces != nil && this.TraceHandler != nil {
                this.TraceHandler(block.Traces.AsTransactionTracesV0())
            }

            if block.ThisBlock.BlockNum + 1 >= this.EndBlock {
                return this.SendCloseMessage()
            }

            break
        }

        status, ok := msg.Impl.(*ship.GetStatusResultV0)
        if ok {
            if this.StatusHandler != nil {
                this.StatusHandler(status)
            }
        }
        break
    }

    return nil
}

func (this *ShipClient) SendCloseMessage() (*ShipClientError) {

    if this.sock == nil {
        return &ShipClientError{ErrNotConnected, "Socket not connected"}
    }

    err := this.sock.WriteMessage(ws.CloseMessage, ws.FormatCloseMessage(ws.CloseNormalClosure, ""))
    if err != nil {
        return &ShipClientError{ErrSendClose, err.Error()}
    }

    return nil
}

func (this *ShipClient) IsOpen() bool {
    return this.sock != nil
}

func (this *ShipClient) Close() (*ShipClientError) {

    if this.sock == nil {
        return &ShipClientError{ErrNotConnected, "Socket not connected"}
    }

    this.sock.Close()
    this.sock = nil

    if this.CloseHandler != nil {
        this.CloseHandler()
    }

    return nil
}
