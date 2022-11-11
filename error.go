
package eos_ship_client

import (
    "fmt"
)

const (
    ErrNotConnected     = 1
    ErrSockRead         = 2
    ErrSendClose        = 3
    ErrACK              = 4
    ErrParse            = 5
)

type ShipClientError struct {
    Type int
    Text string
}

func (e ShipClientError) Error() string {

    var t string

    switch e.Type {
    case ErrNotConnected : t = "not connected"
    case ErrSockRead : t = "socket read"
    case ErrSendClose : t = "send close"
    case ErrACK : t = "ack send"
    case ErrParse : t = "parse"
    }

    return fmt.Sprintf("shipclient - %s: %s", t, e.Text)
}
