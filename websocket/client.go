package websocket

import (
	"bytes"
	"context"
	"errors"
	"sync"

	eos "github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	ws "github.com/gorilla/websocket"
)

var (
	ErrNotConnected          = errors.New("Socket not connected")
	ErrDecodeABI             = errors.New("Failed to decode ABI")
	ErrExpectedABI           = errors.New("expected abi message")
	ErrExpectedBinaryMessage = errors.New("expected binary message")
)

// Client is a low-level SHIP websocket client.
type Client struct {
	// Websocket connection
	conn *ws.Conn

	// Channel to be used to signal that the websocket was closed correctly.
	close chan interface{}

	// Mutex to only allow one thread to close the connection (and close channel)
	close_mu sync.Mutex

	// True if we care about the abi.
	FetchABI bool

	// Pointer to the ABI
	ABI *eos.ABI
}

type Option func(*Client)

// Create a new client
func NewClient(options ...Option) *Client {
	c := &Client{}
	for _, opt := range options {
		opt(c)
	}
	return c
}

func WithFetchABI(value bool) Option {
	return func(c *Client) {
		c.FetchABI = value
	}
}

// Returns true if the websocket connection is open. false otherwise.
func (c *Client) IsOpen() bool {
	return c.conn != nil
}

// Connect connects to a ship node
//
// Url must be of the form schema://host[:port]
// and schema should be "ws" or "wss"
//
// The provided Context must be non-nil.
// If the context expires or is canceled before the connection is complete, an error is returned.
func (c *Client) Connect(ctx context.Context, url string) error {
	conn, _, err := ws.DefaultDialer.DialContext(ctx, url, nil)
	if err == nil {
		c.conn = conn
		c.close = make(chan interface{})

		err = c.readAbi()
	}

	return err
}

func (c *Client) readAbi() error {
	msg_type, data, err := c.conn.ReadMessage()
	if err != nil {
		return err
	}

	if msg_type != ws.TextMessage {
		return ErrExpectedABI
	}

	// Decode and store abi if user cares about it.
	if c.FetchABI {
		abi, err := eos.NewABI(bytes.NewReader(data))
		if err != nil {
			return ErrDecodeABI
		}
		c.ABI = abi
	}

	return nil
}

// Read a result message from the websocket.
//
// This function will block until atleast one message is read or an error occurred.
func (c *Client) Read() (ship.Result, error) {
	r := ship.Result{}

	if !c.IsOpen() {
		return r, ErrNotConnected
	}

	// Read message from socket.
	msg_type, data, err := c.conn.ReadMessage()
	if err != nil {
		// Any type of error should be considered permanent.
		// therefore we can close the socket here
		if close_err := c.Close(); close_err != nil && close_err != ErrNotConnected {
			err = close_err
		}
		return r, err
	}

	if msg_type != ws.BinaryMessage {
		return r, errors.New("xxx")
	}

	// Unpack the message
	if err = eos.UnmarshalBinary(data, &r); err != nil {
		return r, err
	}

	return r, err
}

// Write a request to the ship server.
func (c *Client) Write(req ship.Request) error {
	if !c.IsOpen() {
		return ErrNotConnected
	}

	// Encode the request.
	bytes, err := eos.MarshalBinary(req)
	if err != nil {
		return err
	}

	// Send the request.
	return c.conn.WriteMessage(ws.BinaryMessage, bytes)
}

// Write a websocket close message to the server.
func (c *Client) WriteClose(code int, reason string) error {
	msg := ws.FormatCloseMessage(code, reason)
	return c.conn.WriteMessage(ws.CloseMessage, msg)
}

// Shutdown closes the connection gracefully by sending a Close handshake.
// This function will block until a close message is received from the server an error occure or timeout is exceeded.
func (c *Client) Shutdown(ctx context.Context) error {
	if !c.IsOpen() {
		return ErrNotConnected
	}

	if err := c.WriteClose(ws.CloseNormalClosure, ""); err != nil {
		return err
	}

	// Spawn a go routine that will read until error (close message received)
	go func() {
		for {
			_, err := c.Read()
			if err != nil {
				c.Close()
				return
			}
		}
	}()

	// Wait for connection to fully close.
	select {
	case <-c.close:
		return nil
	case <-ctx.Done():
		c.Close()
		return ctx.Err()
	}
}

// Close the socket on the client side.
//
// NOTE: This method closes the underlying network connection without
// sending any close message.
func (c *Client) Close() error {
	// Obtain mutex lock before checking c.IsOpen()
	// so other threads bails out once they unblocks.
	c.close_mu.Lock()
	defer c.close_mu.Unlock()

	if !c.IsOpen() {
		return ErrNotConnected
	}

	err := c.conn.Close()
	c.conn = nil

	close(c.close)

	return err
}
