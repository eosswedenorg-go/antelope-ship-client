package antelope_ship_client

import (
	"fmt"
)

const (
	ErrNotConnected = iota
	ErrSockRead
	ErrSockClosed
	ErrSendClose
	ErrACK
	ErrParse
)

type ShipClientError struct {
	Type int
	Text string
}

func (e ShipClientError) Error() string {
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
