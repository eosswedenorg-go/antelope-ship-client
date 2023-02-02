package antelope_ship_client

import (
	"errors"
	"fmt"
	"net"

	ws "github.com/gorilla/websocket"
)

const (
	// Not connected to websocket.
	ErrNotConnected = iota

	// Failed to read from websocket
	ErrSockRead

	// Failed to send/read from websocket because it's closed.
	ErrSockClosed

	// Failed to send close message
	ErrSendClose

	// Failed to send Ack message
	ErrACK

	// Failed to parse message
	ErrParse
)

type ClientError struct {
	Type int
	Text string
}

func (e ClientError) Error() string {
	var t string

	switch e.Type {
	case ErrNotConnected:
		t = "not connected"
	case ErrSockClosed:
		t = "socket closed"
	case ErrSockRead:
		t = "socket read"
	case ErrSendClose:
		t = "send close"
	case ErrACK:
		t = "ack send"
	case ErrParse:
		t = "parse"
	}

	msg := fmt.Sprintf("shipclient - %s", t)
	if len(e.Text) > 0 {
		msg = msg + fmt.Sprintf(": %s", e.Text)
	}
	return msg
}

// newClientError creates a new ClientError based on the passed error.
// err_type is used as default type if this function don't find a more appropriate one
// while analysing the error
func newClientError(err error, err_type int) ClientError {
	// Set ErrSockClosed and a more generic message for net.ErrClosed
	if errors.Is(err, net.ErrClosed) {
		return ClientError{ErrSockClosed, "use of closed connection"}
	}

	// Websocket close errors should have ErrSockClosed type.
	if ws.IsUnexpectedCloseError(err) {
		err_type = ErrSockClosed
	}

	return ClientError{err_type, err.Error()}
}
