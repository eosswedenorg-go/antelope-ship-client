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
	"errors"
	"sync/atomic"
	"time"

	"github.com/eosswedenorg-go/antelope-ship-client/websocket"
	ws "github.com/gorilla/websocket"
	"github.com/pnx/antelope-go/chain"
	"github.com/pnx/antelope-go/ship"
)

var ErrEndBlockReached = errors.New("ship: end block reached")

const NULL_BLOCK_NUMBER uint32 = 0xffffffff

type (
	InitFn       func(*chain.Abi)
	BlockFn      func(*ship.GetBlocksResultV0)
	TraceFn      func(*ship.TransactionTraceArray)
	TableDeltaFn func(*ship.TableDeltaArray)
	StatusFn     func(*ship.GetStatusResultV0)
	CloseFn      func()
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

	inShutdown atomic.Bool

	// Callback functions
	InitHandler       InitFn
	BlockHandler      BlockFn
	TraceHandler      TraceFn
	TableDeltaHandler TableDeltaFn
	StatusHandler     StatusFn
	CloseHandler      CloseFn
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

// Option to include traces without using a specific trace handler
func WithTraces() Option {
	return func(s *Stream) {
		s.TraceHandler = func(*ship.TransactionTraceArray) {}
	}
}

// Option to set Stream.BlockHandler
func WithBlockHandler(value BlockFn) Option {
	return func(s *Stream) {
		s.BlockHandler = value
	}
}

func WithTableDeltaHandler(value TableDeltaFn) Option {
	return func(s *Stream) {
		s.TableDeltaHandler = value
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
		FetchDeltas:         s.TableDeltaHandler != nil,
		HavePositions:       nil,
	}
}

// Send a blocks request to the ship server.
// This tells the server to start sending block message to the client.
func (s *Stream) SendBlocksRequest() error {
	return s.client.Write(ship.Request{
		BlocksRequest: s.blockRequest(),
	})
}

// Send a status request to the ship server.
// This tells the server to start sending status message to the client.
func (s *Stream) SendStatusRequest() error {
	return s.client.Write(ship.Request{
		StatusRequest: &ship.GetStatusRequestV0{},
	})
}

func (s *Stream) routeBlock(block *ship.GetBlocksResultV0) {
	if s.BlockHandler != nil {
		s.BlockHandler(block)
	}

	if block.Traces != nil && s.TraceHandler != nil {
		s.TraceHandler(block.Traces)
	}

	if block.Deltas != nil && s.TableDeltaHandler != nil {
		s.TableDeltaHandler(block.Deltas)
	}
}

// Run starts the stream.
// Messages from the server is read and forwarded to the appropriate callback function.
// This function will block until an error occur or the stream is closed.
// Either way the return value is never nil.
func (s *Stream) Run() error {
	var endblockreached bool = false // End of stream

	for {
		result, err := s.client.Read()
		if err != nil {
			if ws.IsCloseError(err, ws.CloseNormalClosure) && endblockreached {
				err = ErrEndBlockReached
			}

			if s.inShutdown.Load() == false {
				_ = s.Shutdown()
			}

			return err
		}

		// Parse message and route to correct callback.
		if result.BlocksResult != nil {
			block := result.BlocksResult

			if block.ThisBlock == nil {
				continue
			}

			s.routeBlock(block)

			if block.ThisBlock.BlockNum+1 >= s.EndBlock {
				// Send Close message, ignore errors here as we
				// should resume reading from the socket.
				_ = s.client.WriteClose(ws.CloseNormalClosure, "end block reached")

				// Signal that the stream was closed due to end block reached.
				endblockreached = true
			}
		}

		if s.StatusHandler != nil && result.StatusResult != nil {
			s.StatusHandler(result.StatusResult)
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

	s.inShutdown.Store(true)
	defer s.inShutdown.Store(false)
	return s.client.Shutdown(ctx)
}
