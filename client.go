/*
# Callback functions

This library uses callback functions to allow the users to execute the code
needed when certain events are triggered.

the Client struct accepts the following callback functions

	InitHandler func(*eos.ABI)

Called when the client receives the first message from the SHIP node on connection.
This message contains the abi with all the information about the functions/types exposed by the websocket api.

	BlockHandler func(*ship.GetBlocksResultV0)

Called when the client reveives a block message from the server.

	TraceHandler func([]*ship.TransactionTraceV0)

When the client reveives a block message from the server and the
Block has any traces attached to it. these traces are then passed to this callback.

	StatusHandler func(*ship.GetStatusResultV0)

Called when the client reveives a status message.

	CloseHandler func()

Called when a client has closed the socket connection (in `(*ShipClient) Close()` function)
*/
package antelope_ship_client

import (
	"bytes"
	"context"
	"sync"
	"time"

	eos "github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	ws "github.com/gorilla/websocket"
)

const NULL_BLOCK_NUMBER uint32 = 0xffffffff

var errNotConnected = ClientError{ErrNotConnected, "Socket not connected"}

type (
	InitFn   func(*eos.ABI)
	BlockFn  func(*ship.GetBlocksResultV0)
	TraceFn  func([]*ship.TransactionTraceV0)
	StatusFn func(*ship.GetStatusResultV0)
	CloseFn  func()
)

type Client struct {
	// Socket connection
	sock *ws.Conn

	// Counter for how many non-ACKed messages we have received.
	unconfirmed uint32

	// Channel to be used to signal that the websocket was closed correctly.
	close chan interface{}

	// Mutex to only allow one thread to close the connection (and close channel)
	close_mu sync.Mutex

	// Specifies the duration for the connection to be established before the client bails out.
	ConnectTimeout time.Duration

	// Specifies the duration for Shutdown() to wait before forcefully disconnecting the socket.
	ShutdownTimeout time.Duration

	// Block to start receiving notifications on.
	StartBlock uint32

	// Block to end receiving notifications on.
	EndBlock uint32

	// if only irreversible blocks should be sent.
	IrreversibleOnly bool

	// Max number of non-ACKed messages that may be sent.
	MaxMessagesInFlight uint32

	// Callback functions
	InitHandler   InitFn
	BlockHandler  BlockFn
	TraceHandler  TraceFn
	StatusHandler StatusFn
	CloseHandler  CloseFn
}

type Option func(*Client)

// Create a new client
func NewClient(options ...Option) *Client {
	c := &Client{
		ConnectTimeout:      time.Second * 30,
		ShutdownTimeout:     time.Second * 4,
		EndBlock:            NULL_BLOCK_NUMBER,
		MaxMessagesInFlight: 10,
	}

	for _, opt := range options {
		opt(c)
	}
	return c
}

// Option to set Client.ConnectTimeout
func WithConnectTimeout(value time.Duration) Option {
	return func(c *Client) {
		c.ConnectTimeout = value
	}
}

// Option to set Client.ShutdownTimeout
func WithShutdownTimeout(value time.Duration) Option {
	return func(c *Client) {
		c.ShutdownTimeout = value
	}
}

// Option to set Client.StartBlock
func WithStartBlock(value uint32) Option {
	return func(c *Client) {
		c.StartBlock = value
	}
}

// Option to set Client.EndBlock
func WithEndBlock(value uint32) Option {
	return func(c *Client) {
		c.EndBlock = value
	}
}

// Option to set Client.IrreversibleOnly
func WithIrreversibleOnly(value bool) Option {
	return func(c *Client) {
		c.IrreversibleOnly = value
	}
}

// Option to set Client.MaxMessagesInFlight
func WithMaxMessagesInFlight(value uint32) Option {
	return func(c *Client) {
		c.MaxMessagesInFlight = value
	}
}

// Option to set Client.InitHandler
func WithInitHandler(value InitFn) Option {
	return func(c *Client) {
		c.InitHandler = value
	}
}

// Option to set Client.TraceHandler
func WithTraceHandler(value TraceFn) Option {
	return func(c *Client) {
		c.TraceHandler = value
	}
}

// Option to set Client.BlockHandler
func WithBlockHandler(value BlockFn) Option {
	return func(c *Client) {
		c.BlockHandler = value
	}
}

// Option to set Client.StatusHandler
func WithStatusHandler(value StatusFn) Option {
	return func(c *Client) {
		c.StatusHandler = value
	}
}

// Option to set Client.CloseHandler
func WithCloseHandler(value CloseFn) Option {
	return func(c *Client) {
		c.CloseHandler = value
	}
}

// Connect connects to a ship node.
// Url must be of the form schema://host[:port]
// and schema should be "ws" or "wss"

// Connect uses context.Background internally; to specify the context, use ConnectContext.
func (c *Client) Connect(url string) error {
	return c.ConnectContext(context.Background(), url)
}

// ConnectContext connects to a ship node using the provided context
//
// The provided Context must be non-nil.
// If the context expires or is canceled before the connection is complete, an error is returned.
func (c *Client) ConnectContext(ctx context.Context, url string) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, c.ConnectTimeout)
	defer cancel()

	// ws package does context timeout if HandshakeTimeout is set.
	// as we provide our own context with timeout. we can skip this.
	dailer := ws.Dialer{
		HandshakeTimeout: 0,
	}

	sock, _, err := dailer.DialContext(ctx, url, nil)
	if err == nil {
		c.sock = sock
		c.close = make(chan interface{})
	}
	return err
}

// Returns the number of messages the client has received
// but have not yet been confirmed as having been received by the client
func (c Client) UnconfirmedMessages() uint32 {
	return c.unconfirmed
}

func (c *Client) blockRequest() *ship.GetBlocksRequestV0 {
	return &ship.GetBlocksRequestV0{
		StartBlockNum:       c.StartBlock,
		EndBlockNum:         c.EndBlock,
		MaxMessagesInFlight: c.MaxMessagesInFlight,
		IrreversibleOnly:    c.IrreversibleOnly,
		FetchBlock:          true,
		FetchTraces:         c.TraceHandler != nil,
		FetchDeltas:         false,
		HavePositions:       []*ship.BlockPosition{},
	}
}

// Send a blocks request to the ship server.
// This tells the server to start sending block message to the client.
func (c *Client) SendBlocksRequest() error {
	return c.send(ship.Request{
		BaseVariant: eos.BaseVariant{
			TypeID: ship.RequestVariant.TypeID("get_blocks_request_v0"),
			Impl:   c.blockRequest(),
		},
	})
}

// Send a status request to the ship server.
// This tells the server to start sending status message to the client.
func (c *Client) SendStatusRequest() error {
	return c.send(ship.Request{
		BaseVariant: eos.BaseVariant{
			TypeID: ship.RequestVariant.TypeID("get_status_request_v0"),
			Impl:   &ship.GetStatusRequestV0{},
		},
	})
}

func (c *Client) send(req ship.Request) error {
	if !c.IsOpen() {
		return errNotConnected
	}
	// Encode the request.
	bytes, err := eos.MarshalBinary(req)
	if err != nil {
		return err
	}

	// Send the request.
	return c.sock.WriteMessage(ws.BinaryMessage, bytes)
}

// Read messages from the client and calls the appropriate callback function.
//
// This function will block until atleast one valid message is processed or an error occured.
func (c *Client) Read() error {
	for {
		var msg ship.Result

		msg_type, data, err := c.Recv()
		if err != nil {
			return err
		}

		if msg_type == ws.TextMessage {
			if c.InitHandler != nil {
				abi, err := eos.NewABI(bytes.NewReader(data))
				if err != nil {
					return ClientError{ErrParse, "Failed to decode ABI"}
				}

				c.InitHandler(abi)
			}
			break
		}

		if msg_type != ws.BinaryMessage {
			return ClientError{ErrParse, "Can only decode binary messages"}
		}

		// Unpack the message
		if err = eos.UnmarshalBinary(data, &msg); err != nil {
			return ClientError{ErrParse, err.Error()}
		}

		// Parse message and route to correct callback.
		if block, ok := msg.Impl.(*ship.GetBlocksResultV0); ok {

			if block.ThisBlock == nil && block.Head != nil {
				continue
			}

			if c.BlockHandler != nil {
				c.BlockHandler(block)
			}

			if block.Traces != nil && len(block.Traces.Elem) > 0 && c.TraceHandler != nil {
				c.TraceHandler(block.Traces.AsTransactionTracesV0())
			}

			if block.ThisBlock.BlockNum+1 >= c.EndBlock {
				// Send Close message, ignore errors here as we
				// should resume reading from the socket.
				_ = c.sendClose(ws.CloseNormalClosure, "end block reached")
				continue
			}

			break
		}

		if c.StatusHandler != nil {
			if status, ok := msg.Impl.(*ship.GetStatusResultV0); ok {
				c.StatusHandler(status)
			}
		}
		break
	}

	return nil
}

func (c *Client) Recv() (int, []byte, error) {
	if !c.IsOpen() {
		return -1, nil, errNotConnected
	}

	// Read message from socket.
	msg_type, data, err := c.sock.ReadMessage()
	if err != nil {

		// Any type of error should be considered permanent.
		// therefore we can close the socket here
		if close_err := c.Close(); close_err != nil && close_err != errNotConnected {
			err = close_err
		} else {
			err = newClientError(err, ErrSockRead)
		}

		return msg_type, data, err
	}

	// Check if we need to ack messages
	c.unconfirmed += 1
	if c.unconfirmed >= c.MaxMessagesInFlight {
		err = c.SendACK()
		if err != nil {
			return msg_type, data, err
		}
	}

	return msg_type, data, nil
}

// Sends an Acknowledgment message that tells the
// server that we have received X number of messages
// where X is the number returned by c.UnconfirmedMessages().
//
// This is normally called internally by Recv()
// So only use this manually if you know that you need to.
func (c *Client) SendACK() error {
	req := ship.NewGetBlocksAck(c.unconfirmed)
	err := c.sock.WriteMessage(ws.BinaryMessage, req)
	c.unconfirmed = 0
	if err != nil {
		return ClientError{ErrSendACK, err.Error()}
	}
	return nil
}

// Send a close message to the server.
func (c *Client) sendClose(code int, reason string) error {
	msg := ws.FormatCloseMessage(code, reason)
	return c.sock.WriteMessage(ws.CloseMessage, msg)
}

// Shutdown closes the connection gracefully by sending a Close handshake.
// This function will block until a close message is received from the server an error occure or timeout is exceeded.
func (c *Client) Shutdown() error {
	if !c.IsOpen() {
		return errNotConnected
	}

	if err := c.sendClose(ws.CloseNormalClosure, ""); err != nil {
		return ClientError{ErrSendClose, err.Error()}
	}

	// Wait for connection to fully close.
	select {
	case <-c.close:
		return nil
	case <-time.After(c.ShutdownTimeout):
		return ClientError{ErrSockClosed, "timeout"}
	}
}

// Returns true if the websocket connection is open. false otherwise.
func (c *Client) IsOpen() bool {
	return c.sock != nil
}

// Close the socket on the client side.
//
// NOTE: This method closes the underlying network connection without
// sending or close message.
func (c *Client) Close() error {
	// Obtain mutex lock before checking c.IsOpen()
	// so other threads bails out once they unblocks.
	c.close_mu.Lock()
	if !c.IsOpen() {
		c.close_mu.Unlock()
		return errNotConnected
	}

	err := c.sock.Close()
	c.sock = nil

	close(c.close)
	c.close_mu.Unlock()

	if c.CloseHandler != nil {
		c.CloseHandler()
	}

	if err != nil {
		return newClientError(err, ErrSockClosed)
	}
	return nil
}
