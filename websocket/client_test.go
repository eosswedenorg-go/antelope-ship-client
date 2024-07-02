package websocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/shufflingpixels/antelope-go/chain"
	"github.com/shufflingpixels/antelope-go/ship"

	"gotest.tools/v3/assert"
)

func wsHandler(t *testing.T, chain ...func(*ws.Conn)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var upgrader ws.Upgrader
		conn, err := upgrader.Upgrade(w, r, http.Header{})
		if err != nil {
			t.Logf("Upgrade: %v", err)
			return
		}
		defer conn.Close()

		for _, f := range chain {
			f(conn)
		}
	}
}

func sendABI(t *testing.T, abi []byte) func(*ws.Conn) {
	return func(conn *ws.Conn) {
		// Send abi.
		err := conn.WriteMessage(ws.TextMessage, abi)
		assert.NilError(t, err)
	}
}

func wsHandlerFunc(t *testing.T, chain ...func(*ws.Conn)) http.HandlerFunc {
	chain = append([]func(*ws.Conn){sendABI(t, []byte(`{"version":"test"}`))}, chain...)
	return wsHandler(t, chain...)
}

type wsServer struct {
	*httptest.Server
	URL string
}

func newWSServer(handler http.Handler) *wsServer {
	var s wsServer
	s.Server = httptest.NewServer(handler)
	s.URL = "ws" + strings.TrimPrefix(s.Server.URL, "http")
	return &s
}

func TestClient_ConnectFail(t *testing.T) {
	client := NewClient()
	err := client.Connect(context.Background(), "ws://127.0.0.255:12334")
	assert.ErrorIs(t, err, syscall.ECONNREFUSED)
}

func TestClient_ConnectTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	client := NewClient()
	err := client.Connect(ctx, "ws://99.99.99.99:9999")
	assert.Error(t, err, "dial tcp 99.99.99.99:9999: i/o timeout")
}

func TestClient_ConnectContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient()
	defer cancel()

	// Start go routine that cancels the context.
	go func() {
		// Wait some time for client.Connect() to happen.
		time.Sleep(time.Millisecond * 100)
		cancel()
	}()

	err := client.Connect(ctx, "ws://99.99.99.99:9999")

	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Error(t, errors.Unwrap(err), "operation was canceled")
}

func TestClient_ConnectFetchABI(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		_, _, _ = c.ReadMessage()
	}))
	defer s.Close()

	client := NewClient(WithFetchABI(true))
	assert.Equal(t, client.FetchABI, true)

	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	assert.Assert(t, client.ABI != nil)
	assert.Equal(t, client.ABI.Version, "test")
}

func TestClient_ConnectNoAbi(t *testing.T) {
	t.Run("BinaryMessage", func(t *testing.T) {
		s := newWSServer(wsHandler(t, func(c *ws.Conn) {
			// Dont send any abi, send a binary message directly.
			err := c.WriteMessage(ws.BinaryMessage, []byte(`some message`))
			assert.NilError(t, err)

			_, _, _ = c.ReadMessage()
		}))
		defer s.Close()

		client := NewClient()

		err := client.Connect(context.Background(), s.URL)
		assert.ErrorIs(t, err, ErrExpectedABI)
	})

	t.Run("Nothing", func(t *testing.T) {
		s := newWSServer(wsHandler(t, func(c *ws.Conn) {}))
		defer s.Close()

		client := NewClient()

		err := client.Connect(context.Background(), s.URL)

		// Server closes immediately
		assert.Assert(t, ws.IsCloseError(err, 1006))
	})
}

func TestClient_ConnectFetchABIDecodeFails(t *testing.T) {
	s := newWSServer(wsHandler(t, func(c *ws.Conn) {
		err := c.WriteMessage(ws.TextMessage, []byte{})
		assert.NilError(t, err)

		_, _, _ = c.ReadMessage()
	}))
	defer s.Close()

	client := NewClient(WithFetchABI(true))
	assert.Equal(t, client.FetchABI, true)

	err := client.Connect(context.Background(), s.URL)
	assert.ErrorIs(t, err, ErrDecodeABI)
}

func TestClient_ReadNonBinaryMessage(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		_ = c.WriteMessage(ws.TextMessage, []byte("This is a text message"))
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	_, err = client.Read()
	assert.ErrorIs(t, err, ErrExpectedBinaryMessage)
}

func TestClient_ReadFromNormalClosedSocket(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		// Just read from the socket to confirm close handshake.
		_, _, err := c.ReadMessage()
		assert.Assert(t, ws.IsCloseError(err, 1000))
		// assert.Error(t, err, "websocket: close 1000 (normal)")
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	err = client.WriteClose(ws.CloseNormalClosure, "")
	assert.NilError(t, err)

	_, err = client.Read()
	// assert.Error(t, err, "websocket: close 1000 (normal)")
	assert.Assert(t, ws.IsCloseError(err, 1000))
}

func TestClient_ReadFromAbnormalClosedSocket(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		c.Close()
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	_, err = client.Read()
	// assert.Error(t, err, "websocket: close 1006 (abnormal closure): unexpected EOF")
	assert.Assert(t, ws.IsCloseError(err, 1006))
}

func TestClient_Read(t *testing.T) {
	expected := ship.Result{
		StatusResult: &ship.GetStatusResultV0{
			Head: &ship.BlockPosition{
				BlockNum: 5000,
				BlockID:  chain.Checksum256{0x0b, 0x10, 0x07, 0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e, 0xbe, 0x64, 0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc, 0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63, 0x7a, 0xc8, 0x52, 0x84, 0x07},
			},
			LastIrreversible: &ship.BlockPosition{
				BlockNum: 6000,
				BlockID:  chain.Checksum256{0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64, 0x11, 0x7a, 0xbe, 0xc0, 0x0a, 0x41, 0x11, 0x78, 0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76, 0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb, 0xdf, 0xa9, 0xe6},
			},
			TraceBeginBlock:      500,
			TraceEndBlock:        600,
			ChainStateBeginBlock: 400,
			ChainStateEndBlock:   500,
		},
	}

	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		data := []byte{
			0x00, 0x88, 0x13, 0x00, 0x00, 0x0b, 0x10, 0x07,
			0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e,
			0xbe, 0x64, 0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc,
			0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63,
			0x7a, 0xc8, 0x52, 0x84, 0x07, 0x70, 0x17, 0x00,
			0x00, 0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64,
			0x11, 0x7a, 0xbe, 0xc0, 0x0a, 0x41, 0x11, 0x78,
			0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76,
			0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb, 0xdf, 0xa9,
			0xe6, 0xf4, 0x01, 0x00, 0x00, 0x58, 0x02, 0x00,
			0x00, 0x90, 0x01, 0x00, 0x00, 0xf4, 0x01, 0x00,
			0x00,
		}

		err := c.WriteMessage(ws.BinaryMessage, data)
		assert.NilError(t, err)
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	msg, err := client.Read()
	assert.NilError(t, err)

	assert.DeepEqual(t, msg, expected)
}

func TestClient_Write(t *testing.T) {
	ch := make(chan interface{})

	req := ship.Request{
		BlocksRequest: &ship.GetBlocksRequestV0{
			StartBlockNum:       1000,
			EndBlockNum:         2000,
			MaxMessagesInFlight: 20,
			HavePositions:       []*ship.BlockPosition{},
			IrreversibleOnly:    false,
			FetchBlock:          false,
			FetchTraces:         false,
			FetchDeltas:         false,
		},
	}

	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		defer close(ch)

		expectedData := []byte{
			0x01, 0xe8, 0x03, 0x00, 0x00, 0xd0, 0x07, 0x00,
			0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
		}

		msg_type, msg, err := c.ReadMessage()
		assert.NilError(t, err)
		assert.Equal(t, msg_type, ws.BinaryMessage)
		assert.DeepEqual(t, msg, expectedData)
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	err = client.Write(req)
	assert.NilError(t, err)

	<-ch
}

func TestClient_WriteClose(t *testing.T) {
	ch := make(chan bool)

	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		defer close(ch)

		_, _, err := c.ReadMessage()
		assert.Assert(t, ws.IsCloseError(err, 1013))
		assert.Equal(t, err.(*ws.CloseError).Text, "close but no cigar")
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	err = client.WriteClose(ws.CloseTryAgainLater, "close but no cigar")
	assert.NilError(t, err)

	<-ch
}

// Test if connection is cleanly closed on websocket handshake.
func TestClient_WSCloseHandshake(t *testing.T) {
	// handshake initiated by the server.
	t.Run("ServerSide", func(t *testing.T) {
		ch := make(chan bool)

		s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
			defer close(ch)

			// NOTE: In gorilla/websockets version 1.5.1, the default close handler now returns
			// the error from their attempt to write a close message.
			// However this will return `ErrCloseSent` if a close message has already been sent.
			// To address this issue in the test, a quick fix is to define a null handler.
			c.SetCloseHandler(func(code int, text string) error {
				return nil
			})

			// Server sents a close message.
			frame := ws.FormatCloseMessage(ws.CloseGoingAway, "woops")
			_ = c.WriteMessage(ws.CloseMessage, frame)

			// Next read should return a error with the same close message.
			_, _, err := c.ReadMessage()

			// NOTE: We only assert that a close error is returned here because
			// the specs only requires that the other side sends a close message
			// in return.
			if err == nil {
				t.Errorf("Expected *ws.CloseError but error is nil")
				return
			}
			if _, ok := err.(*ws.CloseError); !ok {
				t.Errorf("Expected *ws.CloseError got: %s (%s)", reflect.TypeOf(err), err)
			}
		}))
		defer s.Close()

		client := NewClient()
		err := client.Connect(context.Background(), s.URL)
		assert.NilError(t, err)

		// Just connected. connection should be open
		assert.Equal(t, client.IsOpen(), true)

		// If we read now, we should get the close message as an error.
		_, err = client.Read()
		assert.Assert(t, ws.IsCloseError(err, 1001))
		assert.Equal(t, err.(*ws.CloseError).Text, "woops")

		// Connection should now be closed.
		assert.Equal(t, client.IsOpen(), false)

		<-ch
	})

	// handshake initiated by the client
	t.Run("ClientSide", func(t *testing.T) {
		s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
			_, _, _ = c.ReadMessage()
		}))
		defer s.Close()

		client := NewClient()
		err := client.Connect(context.Background(), s.URL)
		assert.NilError(t, err)

		// Just connected. connection should be open
		assert.Equal(t, client.IsOpen(), true)

		err = client.WriteClose(ws.CloseNoStatusReceived, "")
		assert.NilError(t, err)

		// Should not be closed yet because we have not consumed any close message
		assert.Equal(t, client.IsOpen(), true)

		// If we read now, we should get a close error.
		_, err = client.Read()
		assert.Assert(t, ws.IsCloseError(err, 1005))

		// Now we should be closed.
		assert.Equal(t, client.IsOpen(), false)
	})
}

func TestClient_Shutdown(t *testing.T) {
	ch := make(chan bool)

	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		defer close(ch)

		_, _, err := c.ReadMessage()
		assert.Assert(t, ws.IsCloseError(err, 1000))
	}))

	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	// Call read as a go routine to process the close essage
	go client.Read()

	err = client.Shutdown(ctx)
	assert.NilError(t, err)

	<-ch
}

func TestClient_ShutdownContextDeadline(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		// Simulate a slow server by sleeping.
		time.Sleep(time.Second * 10)
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err = client.Shutdown(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestClient_ShutdownContextCancel(t *testing.T) {
	s := newWSServer(wsHandlerFunc(t, func(c *ws.Conn) {
		// Simulate a slow server by sleeping.
		time.Sleep(time.Second * 10)
	}))
	defer s.Close()

	client := NewClient()
	err := client.Connect(context.Background(), s.URL)
	assert.NilError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)

	// Cancel the context immediately
	cancel()

	err = client.Shutdown(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestIsCloseError(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"Normal", 1000},
		{"GoingAway", 1001},
		{"ProtocolError", 1002},
		{"UnsupportedData", 1003},
		{"NoStatusReceived", 1005},
		{"AbnormalClosure", 1006},
		{"InvalidFramePayloadData", 1007},
		{"PolicyViolation", 1008},
		{"MessageTooBig", 1009},
		{"MandatoryExtension", 1010},
		{"InternalServerErr", 1011},
		{"ServiceRestart", 1012},
		{"TryAgainLater", 1013},
		{"TLSHandshake", 1015},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &ws.CloseError{Code: tt.code}
			assert.Equal(t, IsCloseError(err, tt.code), true)
		})
	}

	// Test default parameter
	assert.Equal(t, IsCloseError(&ws.CloseError{Code: 1000}), true)

	// Test that generic error is not a close error.
	assert.Equal(t, IsCloseError(errors.New("test")), false)
}
