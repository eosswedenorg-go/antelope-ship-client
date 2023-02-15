/*
# Callback functions

This library uses callback functions to allow the users to execute the code
needed when certain events are triggered.

the Stream struct accepts the following callback functions

	InitHandler func(*eos.ABI)

Called when the first message on the stream arrives.
This message contains the abi with all the information about the functions/types exposed by the websocket api.

	BlockHandler func(*ship.GetBlocksResultV0)

Called when the stream reveives a block message from the server.

	TraceHandler func([]*ship.TransactionTraceV0)

When the stream reveives a block message from the server and the
Block has any traces attached to it. these traces are then passed to this callback.

	StatusHandler func(*ship.GetStatusResultV0)

Called when the stream reveives a status message.

	CloseHandler func()

Called when a stream has closed the socket connection (in `(*Stream) Shutdown()` function)
*/
package antelope_ship_client

import (
	"context"
	"time"

	eos "github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	"github.com/eosswedenorg-go/antelope-ship-client/websocket"
	ws "github.com/gorilla/websocket"
)

const NULL_BLOCK_NUMBER uint32 = 0xffffffff

type (
	InitFn   func(*eos.ABI)
	BlockFn  func(*ship.GetBlocksResultV0)
	TraceFn  func([]*ship.TransactionTraceV0)
	StatusFn func(*ship.GetStatusResultV0)
	CloseFn  func()
)

type Stream struct {
	// Socket connection
	client websocket.Client

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

	// Callback functions
	InitHandler   InitFn
	BlockHandler  BlockFn
	TraceHandler  TraceFn
	StatusHandler StatusFn
	CloseHandler  CloseFn
}

type Option func(*Stream)

// Create a new stream
func NewStream(options ...Option) *Stream {
	s := &Stream{
		ConnectTimeout:  time.Second * 30,
		ShutdownTimeout: time.Second * 4,
		EndBlock:        NULL_BLOCK_NUMBER,
	}

	for _, opt := range options {
		opt(s)
	}
	return s
}

// Option to set Stream.ConnectTimeout
func WithConnectTimeout(value time.Duration) Option {
	return func(s *Stream) {
		s.ConnectTimeout = value
	}
}

// Option to set Stream.ShutdownTimeout
func WithShutdownTimeout(value time.Duration) Option {
	return func(s *Stream) {
		s.ShutdownTimeout = value
	}
}

// Option to set Stream.StartBlock
func WithStartBlock(value uint32) Option {
	return func(s *Stream) {
		s.StartBlock = value
	}
}

// Option to set Stream.EndBlock
func WithEndBlock(value uint32) Option {
	return func(s *Stream) {
		s.EndBlock = value
	}
}

// Option to set Stream.IrreversibleOnly
func WithIrreversibleOnly(value bool) Option {
	return func(s *Stream) {
		s.IrreversibleOnly = value
	}
}

// Option to set Stream.InitHandler
func WithInitHandler(value InitFn) Option {
	return func(s *Stream) {
		s.InitHandler = value
	}
}

// Option to set Stream.TraceHandler
func WithTraceHandler(value TraceFn) Option {
	return func(s *Stream) {
		s.TraceHandler = value
	}
}

// Option to set Stream.BlockHandler
func WithBlockHandler(value BlockFn) Option {
	return func(s *Stream) {
		s.BlockHandler = value
	}
}

// Option to set Stream.StatusHandler
func WithStatusHandler(value StatusFn) Option {
	return func(s *Stream) {
		s.StatusHandler = value
	}
}

// Option to set Stream.CloseHandler
func WithCloseHandler(value CloseFn) Option {
	return func(s *Stream) {
		s.CloseHandler = value
	}
}

// Connect connects to a ship node.
// Url must be of the form schema://host[:port]
// and schema should be "ws" or "wss"

// Connect uses context.Background internally; to specify the context, use ConnectContext.
func (s *Stream) Connect(url string) error {
	return s.ConnectContext(context.Background(), url)
}

// ConnectContext connects to a ship node using the provided context
//
// The provided Context must be non-nil.
// If the context expires or is canceled before the connection is complete, an error is returned.
func (s *Stream) ConnectContext(ctx context.Context, url string) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, s.ConnectTimeout)
	defer cancel()

	if s.InitHandler != nil {

		// Tell client that we want the abi.
		s.client.FetchABI = true

		// defer init handler call
		defer func() {
			if s.client.ABI != nil {
				s.InitHandler(s.client.ABI)
			}
		}()
	}

	return s.client.Connect(ctx, url)
}

func (s *Stream) blockRequest() *ship.GetBlocksRequestV0 {
	return &ship.GetBlocksRequestV0{
		StartBlockNum:       s.StartBlock,
		EndBlockNum:         s.EndBlock,
		MaxMessagesInFlight: 0xffffffff,
		IrreversibleOnly:    s.IrreversibleOnly,
		FetchBlock:          s.BlockHandler != nil,
		FetchTraces:         s.TraceHandler != nil,
		FetchDeltas:         false,
		HavePositions:       []*ship.BlockPosition{},
	}
}

// Send a blocks request to the ship server.
// This tells the server to start sending block message to the client.
func (s *Stream) SendBlocksRequest() error {
	return s.client.Write(ship.Request{
		BaseVariant: eos.BaseVariant{
			TypeID: ship.RequestVariant.TypeID("get_blocks_request_v0"),
			Impl:   s.blockRequest(),
		},
	})
}

// Send a status request to the ship server.
// This tells the server to start sending status message to the client.
func (s *Stream) SendStatusRequest() error {
	return s.client.Write(ship.Request{
		BaseVariant: eos.BaseVariant{
			TypeID: ship.RequestVariant.TypeID("get_status_request_v0"),
			Impl:   &ship.GetStatusRequestV0{},
		},
	})
}

// Run starts the stream.
// Messages from the server is read and forwarded to the appropriate callback function.
// This function will block until an error occur or the stream is closed.
// Eiter way the return value is never nil.
func (s *Stream) Run() error {
	for {
		result, err := s.client.Read()
		if err != nil {
			_ = s.Shutdown()
			return err
		}

		// Parse message and route to correct callback.
		if block, ok := result.Impl.(*ship.GetBlocksResultV0); ok {

			if block.ThisBlock == nil && block.Head != nil {
				continue
			}

			if s.BlockHandler != nil {
				s.BlockHandler(block)
			}

			if block.Traces != nil && len(block.Traces.Elem) > 0 && s.TraceHandler != nil {
				s.TraceHandler(block.Traces.AsTransactionTracesV0())
			}

			if block.ThisBlock.BlockNum+1 >= s.EndBlock {
				// Send Close message, ignore errors here as we
				// should resume reading from the socket.
				_ = s.client.WriteClose(ws.CloseNormalClosure, "end block reached")
			}
		}

		if s.StatusHandler != nil {
			if status, ok := result.Impl.(*ship.GetStatusResultV0); ok {
				s.StatusHandler(status)
			}
		}
	}
}

// Shutdown closes the stream gracefully by performing a websocket close handshake.
// This function will block until a close message is received from the server an error occure or timeout is exceeded.
func (s *Stream) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()

	if s.CloseHandler != nil {
		defer s.CloseHandler()
	}

	return s.client.Shutdown(ctx)
}
