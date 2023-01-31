package antelope_ship_client

import (
	"errors"
	"net"
	"reflect"
	"testing"

	ws "github.com/gorilla/websocket"
)

func Test_newClientError(t *testing.T) {
	type args struct {
		err      error
		err_type int
	}
	tests := []struct {
		name string
		args args
		want ClientError
	}{
		{"Generic", args{errors.New("some message"), ErrACK}, ClientError{Text: "some message", Type: ErrACK}},
		{"net.ErrClosed", args{net.ErrClosed, ErrSockRead}, ClientError{Text: "use of closed connection", Type: ErrSockClosed}},
		{"net.ErrWriteToConnected", args{net.ErrWriteToConnected, ErrSockRead}, ClientError{Text: net.ErrWriteToConnected.Error(), Type: ErrSockRead}},
		{"ws.CloseNormalClosure", args{&ws.CloseError{Code: ws.CloseNormalClosure}, ErrNotConnected}, ClientError{Text: "websocket: close 1000 (normal)", Type: ErrSockClosed}},
		{"ws.CloseGoingAway", args{&ws.CloseError{Code: ws.CloseGoingAway}, ErrSockRead}, ClientError{Text: "websocket: close 1001 (going away)", Type: ErrSockClosed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newClientError(tt.args.err, tt.args.err_type); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newClientError() = %v, want %v", got, tt.want)
			}
		})
	}
}