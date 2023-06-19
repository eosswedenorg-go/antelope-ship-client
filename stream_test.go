package antelope_ship_client

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/eoscanada/eos-go/ship"
	ws "github.com/gorilla/websocket"

	"github.com/google/go-cmp/cmp/cmpopts"
	"gotest.tools/v3/assert"
)

var upgrader ws.Upgrader

type testHandler struct {
	t *testing.T

	responses [][]byte

	CloseError bool

	ExpectedBlockRequest  *ship.GetBlocksRequestV0
	ExpectedStatusRequest *ship.GetStatusRequestV0
	RespondBlocks         []ship.GetBlocksResultV0
}

type testServer struct {
	*httptest.Server
	RawURL string
	URL    *url.URL
}

func makeWsProto(s string) string {
	return "ws" + strings.TrimPrefix(s, "http")
}

func newServer(t *testing.T) *testServer {
	return newServerWithHandler(t, &testHandler{t: t})
}

func newServerWithHandler(t *testing.T, handler *testHandler) *testServer {
	var s testServer
	s.Server = httptest.NewServer(handler)
	s.RawURL = makeWsProto(s.Server.URL)
	s.URL, _ = url.Parse(s.RawURL)
	return &s
}

func (h testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wsock, err := upgrader.Upgrade(w, r, http.Header{})
	if err != nil {
		h.t.Logf("Upgrade: %v", err)
		return
	}

	err = wsock.WriteMessage(ws.TextMessage, []byte(`{"version": "test"}`))
	assert.NilError(h.t, err)

	if h.CloseError {
		wsock.Close()
	} else {
		defer wsock.Close()
	}

	for {

		mt, data, err := wsock.ReadMessage()
		if err != nil {
			break
		}

		if mt == ws.BinaryMessage {
			var req ship.Request

			err = eos.UnmarshalBinary(data, &req)
			assert.NilError(h.t, err)

			block_req, ok := req.Impl.(*ship.GetBlocksRequestV0)
			if ok && h.ExpectedBlockRequest != nil {
				assert.DeepEqual(h.t, *h.ExpectedBlockRequest, *block_req)
			}

			status_req, ok := req.Impl.(*ship.GetStatusRequestV0)
			if ok && h.ExpectedStatusRequest != nil {
				assert.DeepEqual(h.t, *h.ExpectedStatusRequest, *status_req)
			}

			for _, bytes := range h.responses {
				err = wsock.WriteMessage(ws.BinaryMessage, bytes)
				assert.NilError(h.t, err)
			}
		}
	}
}

func loadBlock(file string) ([]byte, error) {
	hex_data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(string(hex_data))
}

func TestStream_ConstructWithOptions(t *testing.T) {
	handler := NewStream(WithStartBlock(1234),
		WithEndBlock(5000),
		WithIrreversibleOnly(true),
		WithConnectTimeout(time.Second*15))

	assert.Equal(t, handler.StartBlock, uint32(1234))
	assert.Equal(t, handler.EndBlock, uint32(5000))
	assert.Equal(t, handler.IrreversibleOnly, true)
	assert.Equal(t, handler.ConnectTimeout, time.Second*15)
}

func TestStream_ConstructWithTraces(t *testing.T) {
	assert.Equal(t, true, NewStream(WithTraces()).blockRequest().FetchTraces)
}

func TestStream_ConstructWithCustomOption(t *testing.T) {
	handler := NewStream(func(s *Stream) {
		s.StartBlock = 4000
		s.EndBlock = 5000
		s.ConnectTimeout = time.Minute
	})

	assert.Equal(t, handler.StartBlock, uint32(4000))
	assert.Equal(t, handler.EndBlock, uint32(5000))
	assert.Equal(t, handler.ConnectTimeout, time.Minute)
}

func TestStream_ConnectOK(t *testing.T) {
	s := newServer(t)
	defer s.Close()

	stream := NewStream()
	assert.NilError(t, stream.Connect(s.URL.String()))
}

func TestStream_ConnectFail(t *testing.T) {
	stream := NewStream()
	err := stream.Connect("ws://:9999")
	assert.Error(t, err, "dial tcp :9999: connect: connection refused")
}

func TestStream_ConnectTimeout(t *testing.T) {
	stream := NewStream(WithConnectTimeout(time.Millisecond * 10))
	err := stream.Connect("ws://99.99.99.99:9999")
	assert.Error(t, err, "dial tcp 99.99.99.99:9999: i/o timeout")
}

func TestStream_ConnectContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := NewStream(WithConnectTimeout(time.Minute))
	defer cancel()

	go func() {
		time.Sleep(time.Millisecond * 500)
		cancel()
	}()

	err := stream.ConnectContext(ctx, "ws://99.99.99.99:9999")

	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Error(t, err, "dial tcp 99.99.99.99:9999: operation was canceled")
}

func TestStream_ReadFromNormalClosedSocket(t *testing.T) {
	handler := testHandler{t: t}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	stream := NewStream(WithStartBlock(23617231))
	err := stream.Connect(s.URL.String())
	assert.NilError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		_ = stream.Shutdown()
	}()

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1000 (normal)")
}

func TestStream_ReadFromAbnormalClosedSocket(t *testing.T) {
	handler := testHandler{t: t, CloseError: true}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	stream := NewStream(WithStartBlock(72367186))

	err := stream.Connect(s.URL.String())
	assert.NilError(t, err)

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1006 (abnormal closure): unexpected EOF")
}

func TestStream_ReadIsUnblockedOnShutdown(t *testing.T) {
	handler := testHandler{t: t}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	stream := NewStream(WithStartBlock(53482321))
	err := stream.Connect(s.URL.String())
	assert.NilError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 500)
		err = stream.Shutdown()
		assert.NilError(t, err)
	}()

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1000 (normal)")
}

func TestStream_StatusMessage(t *testing.T) {
	called := false

	expected := ship.GetStatusResultV0{
		Head: &ship.BlockPosition{
			BlockNum: 5000,
			BlockID:  eos.Checksum256{0x0b, 0x10, 0x07, 0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e, 0xbe, 0x64, 0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc, 0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63, 0x7a, 0xc8, 0x52, 0x84, 0x07},
		},
		LastIrreversible: &ship.BlockPosition{
			BlockNum: 6000,
			BlockID:  eos.Checksum256{0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64, 0x11, 0x7a, 0xbe, 0xc0, 0x0a, 0x41, 0x11, 0x78, 0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76, 0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb, 0xdf, 0xa9, 0xe6},
		},
		TraceBeginBlock:      500,
		TraceEndBlock:        600,
		ChainStateBeginBlock: 400,
		ChainStateEndBlock:   500,
	}

	status_handler := func(r *ship.GetStatusResultV0) {
		called = true
		assert.DeepEqual(t, expected, *r)
	}
	stream := NewStream(WithStartBlock(1234), WithStatusHandler(status_handler))

	handler := testHandler{
		t:                     t,
		ExpectedStatusRequest: &ship.GetStatusRequestV0{},
		responses: [][]byte{
			{
				0x00, 0x88, 0x13, 0x00, 0x00, 0x0b, 0x10, 0x07, 0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e,
				0xbe, 0x64, 0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc, 0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63,
				0x7a, 0xc8, 0x52, 0x84, 0x07, 0x70, 0x17, 0x00, 0x00, 0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64,
				0x11, 0x7a, 0xbe, 0xc0, 0x0a, 0x41, 0x11, 0x78, 0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76,
				0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb, 0xdf, 0xa9, 0xe6, 0xf4, 0x01, 0x00, 0x00, 0x58, 0x02, 0x00,
				0x00, 0x90, 0x01, 0x00, 0x00, 0xf4, 0x01, 0x00, 0x00,
			},
		},
	}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	err := stream.Connect(s.URL.String())
	assert.NilError(t, err)
	err = stream.SendStatusRequest()
	assert.NilError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		_ = stream.Shutdown()
	}()

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1000 (normal)")

	assert.Assert(t, called, "Status callback never called")
}

func TestStream_ReadBlockMessages(t *testing.T) {
	called := false

	expected := ship.GetBlocksResultV0{
		Head: &ship.BlockPosition{
			BlockNum: 5000,
			BlockID:  eos.Checksum256{0x0b, 0x10, 0x07, 0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e, 0xbe, 0x64, 0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc, 0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63, 0x7a, 0xc8, 0x52, 0x84, 0x07},
		},
		ThisBlock: &ship.BlockPosition{
			BlockNum: 4000,
			BlockID:  eos.Checksum256{0x0b, 0x10, 0x07, 0xfc, 0xdb, 0x10, 0x84, 0x84, 0xbf, 0x37, 0xd3, 0xdf, 0x62, 0x42, 0x8c, 0xe8, 0x26, 0x23, 0x1b, 0x4d, 0x22, 0x3d, 0x8e, 0x16, 0x89, 0x16, 0x4c, 0x07, 0xb4, 0x0c, 0x7c, 0x27},
		},
		PrevBlock: &ship.BlockPosition{
			BlockNum: 3999,
			BlockID:  eos.Checksum256{0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64, 0x11, 0x7a, 0xbe, 0xc0, 0x0a, 0x41, 0x11, 0x78, 0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76, 0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb, 0xdf, 0xa9, 0xe6},
		},
		LastIrreversible: &ship.BlockPosition{
			BlockNum: 3000,
			BlockID:  eos.Checksum256{0x0b, 0x10, 0x07, 0xfd, 0x16, 0x63, 0x8a, 0xb1, 0xa0, 0xfd, 0x5f, 0xf1, 0xfc, 0xb0, 0xd2, 0x8c, 0x16, 0x77, 0xfc, 0x27, 0x65, 0x69, 0x9d, 0x66, 0x60, 0x81, 0x54, 0xa6, 0x88, 0x9e, 0x55, 0x9b},
		},
	}

	block_handler := func(r *ship.GetBlocksResultV0) {
		called = true
		assert.DeepEqual(t, expected, *r)
	}
	stream := NewStream(WithStartBlock(1234), WithBlockHandler(block_handler))

	handler := testHandler{
		t:                    t,
		ExpectedBlockRequest: stream.blockRequest(),
		responses: [][]byte{
			{
				0x01, 0x88, 0x13, 0x00, 0x00, 0x0b, 0x10, 0x07, 0xfb, 0x2b, 0x23, 0x3b, 0x6b, 0xa8, 0x5f, 0x4e, 0xbe, 0x64,
				0xc4, 0x9e, 0x0f, 0x23, 0xf3, 0xcc, 0x94, 0xcf, 0x9a, 0x9f, 0xcc, 0xa7, 0xbb, 0x63, 0x7a, 0xc8, 0x52, 0x84,
				0x07, 0xb8, 0x0b, 0x00, 0x00, 0x0b, 0x10, 0x07, 0xfd, 0x16, 0x63, 0x8a, 0xb1, 0xa0, 0xfd, 0x5f, 0xf1, 0xfc,
				0xb0, 0xd2, 0x8c, 0x16, 0x77, 0xfc, 0x27, 0x65, 0x69, 0x9d, 0x66, 0x60, 0x81, 0x54, 0xa6, 0x88, 0x9e, 0x55,
				0x9b, 0x01, 0xa0, 0x0f, 0x00, 0x00, 0x0b, 0x10, 0x07, 0xfc, 0xdb, 0x10, 0x84, 0x84, 0xbf, 0x37, 0xd3, 0xdf,
				0x62, 0x42, 0x8c, 0xe8, 0x26, 0x23, 0x1b, 0x4d, 0x22, 0x3d, 0x8e, 0x16, 0x89, 0x16, 0x4c, 0x07, 0xb4, 0x0c,
				0x7c, 0x27, 0x01, 0x9f, 0x0f, 0x00, 0x00, 0x40, 0xa9, 0x62, 0x70, 0xc9, 0x8b, 0x64, 0x11, 0x7a, 0xbe, 0xc0,
				0x0a, 0x41, 0x11, 0x78, 0x10, 0x7c, 0xcd, 0x91, 0x8c, 0x19, 0xfb, 0x76, 0x32, 0xb6, 0x8f, 0x9b, 0xb5, 0xeb,
				0xdf, 0xa9, 0xe6, 0x00, 0x00, 0x00,
			},
		},
	}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	err := stream.Connect(s.URL.String())
	assert.NilError(t, err)
	err = stream.SendBlocksRequest()
	assert.NilError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		_ = stream.Shutdown()
	}()

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1000 (normal)")

	assert.Assert(t, called, "Block callback never called")
}

func TestStream_ReadTraceMessages(t *testing.T) {
	called := false

	// First trace 71f9afc519eab1bcf599bded5848f3167c1603238f4eb0f7998565b559b0b988
	trace0 := ship.TransactionTraceV0{
		ID: eos.Checksum256{
			0x71, 0xf9, 0xaf, 0xc5, 0x19, 0xea, 0xb1, 0xbc, 0xf5, 0x99, 0xbd, 0xed, 0x58, 0x48, 0xf3, 0x16,
			0x7c, 0x16, 0x03, 0x23, 0x8f, 0x4e, 0xb0, 0xf7, 0x99, 0x85, 0x65, 0xb5, 0x59, 0xb0, 0xb9, 0x88,
		},
		Status:        eos.TransactionStatusExecuted,
		CPUUsageUS:    100,
		NetUsage:      0,
		NetUsageWords: 0,
		ActionTraces: []*ship.ActionTrace{
			{
				BaseVariant: eos.BaseVariant{
					TypeID: 1,
					Impl: &ship.ActionTraceV1{
						ActionOrdinal: 1,
						Receipt: &ship.ActionReceipt{
							BaseVariant: eos.BaseVariant{
								Impl: &ship.ActionReceiptV0{
									Receiver: eos.Name("eosio"),
									ActDigest: eos.Checksum256{
										0xd7, 0x8f, 0x98, 0x51, 0xa8, 0x9b, 0x74, 0x8b,
										0x9c, 0xd8, 0xcc, 0xf7, 0x96, 0x38, 0x69, 0x49,
										0x67, 0x90, 0xca, 0x40, 0xfa, 0x70, 0x18, 0xf1,
										0xa7, 0x36, 0x17, 0xaa, 0x8f, 0x33, 0x86, 0x2a,
									},
									GlobalSequence: 357180020394,
									RecvSequence:   353992801,
									AuthSequence:   []ship.AccountAuthSequence{{Account: eos.Name("eosio"), Sequence: 282750771}},
									CodeSequence:   18,
									ABISequence:    19,
								},
							},
						},
						Receiver: eos.Name("eosio"),
						Act: &ship.Action{
							Account:       eos.AccountName("eosio"),
							Name:          eos.ActionName("onblock"),
							Authorization: []eos.PermissionLevel{{Actor: eos.AN("eosio"), Permission: eos.PN("active")}},
							Data: []byte{
								0xc6, 0xea, 0x11, 0x56, 0x40, 0x5d, 0xa6, 0x29,
								0x6a, 0xaa, 0x30, 0x55, 0x00, 0x00, 0x10, 0xa1,
								0xa2, 0xf2, 0x20, 0x05, 0xb1, 0xab, 0x06, 0xb2,
								0x0d, 0x32, 0xc0, 0x79, 0xe7, 0x78, 0x1c, 0x30,
								0xf7, 0x6d, 0xba, 0x48, 0x12, 0x30, 0xa9, 0xa9,
								0x86, 0xfa, 0x23, 0x12, 0xe8, 0xd8, 0xbc, 0x4e,
								0xdc, 0x41, 0x60, 0x82, 0x27, 0x30, 0xa7, 0x57,
								0x76, 0x4a, 0x14, 0x99, 0xbd, 0xa6, 0x08, 0xaa,
								0x8c, 0x91, 0x60, 0x9c, 0x48, 0x81, 0x52, 0xdf,
								0xaf, 0x8e, 0xf0, 0xd6, 0x09, 0x62, 0x20, 0xfc,
								0x50, 0xf6, 0xdb, 0x4c, 0x1e, 0x93, 0x14, 0x88,
								0x33, 0x3d, 0x1f, 0x88, 0x58, 0x57, 0x2c, 0x5d,
								0x6e, 0xd7, 0x8b, 0x67, 0xae, 0x1e, 0xfb, 0xc7,
								0x58, 0x26, 0xed, 0xba, 0x7f, 0x0f, 0x03, 0x08,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						AccountRamDeltas: []*eos.AccountDelta{{Account: eos.AccountName("eosio")}},
						ReturnValue:      []uint8{},
					},
				},
			},
		},
		AccountDelta:    nil,
		Elapsed:         0,
		Scheduled:       false,
		FailedDtrxTrace: nil,
		Partial: &ship.PartialTransaction{
			BaseVariant: eos.BaseVariant{
				Impl: &ship.PartialTransactionV0{
					TransactionExtensions: []*ship.Extension{},
					Signatures:            []ecc.Signature{},
					ContextFreeData:       []uint8{},
				},
			},
		},
	}

	// Second trace 865900f0be1e2027ec9081a1f17022531fd54cfc3277b004f2bebaee1eb0b09f
	trace1 := ship.TransactionTraceV0{
		ID: eos.Checksum256{
			0x86, 0x59, 0x00, 0xf0, 0xbe, 0x1e, 0x20, 0x27, 0xec, 0x90, 0x81, 0xa1, 0xf1, 0x70, 0x22, 0x53,
			0x1f, 0xd5, 0x4c, 0xfc, 0x32, 0x77, 0xb0, 0x04, 0xf2, 0xbe, 0xba, 0xee, 0x1e, 0xb0, 0xb0, 0x9f,
		},
		Status:        eos.TransactionStatusExecuted,
		CPUUsageUS:    653,
		NetUsage:      128,
		NetUsageWords: 16,
		ActionTraces: []*ship.ActionTrace{
			{
				BaseVariant: eos.BaseVariant{
					TypeID: 1,
					Impl: &ship.ActionTraceV1{
						ActionOrdinal: 1,
						Receipt: &ship.ActionReceipt{
							BaseVariant: eos.BaseVariant{
								Impl: &ship.ActionReceiptV0{
									Receiver: eos.Name("noloss111111"),
									ActDigest: eos.Checksum256{
										0x3a, 0x86, 0x2c, 0xe1, 0xee, 0x32, 0xcf, 0x2b,
										0x61, 0x5b, 0x94, 0x60, 0x05, 0xae, 0xd6, 0x7a,
										0x54, 0xc4, 0x4f, 0xc6, 0xde, 0xcf, 0x0b, 0x74,
										0x23, 0x00, 0x17, 0x21, 0x8e, 0x81, 0x4c, 0x37,
									},
									GlobalSequence: 357180020395,
									RecvSequence:   20515194,
									AuthSequence: []ship.AccountAuthSequence{
										{Account: eos.Name("eddiewillers"), Sequence: 315000},
										{Account: eos.Name("taggartdagny"), Sequence: 19100774},
									},
									CodeSequence: 1338,
									ABISequence:  88,
								},
							},
						},
						Receiver: eos.Name("noloss111111"),
						Act: &ship.Action{
							Account: eos.AccountName("noloss111111"),
							Name:    eos.ActionName("trade"),
							Authorization: []eos.PermissionLevel{
								{Actor: eos.AN("eddiewillers"), Permission: eos.PN("active")},
								{Actor: eos.AN("taggartdagny"), Permission: eos.PN("active")},
							},
							Data: []byte{0xe0, 0x27, 0x33, 0x29, 0x5f, 0xc3, 0x98, 0xc9, 0x3A, 0xd6, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00},
						},
						AccountRamDeltas: []*eos.AccountDelta{},
						ReturnValue:      []uint8{},
					},
				},
			},
		},
		AccountDelta:    nil,
		Elapsed:         0,
		Scheduled:       false,
		FailedDtrxTrace: nil,
		Partial: &ship.PartialTransaction{
			BaseVariant: eos.BaseVariant{
				Impl: &ship.PartialTransactionV0{
					Expiration:            1668692254,
					RefBlockNum:           41380,
					RefBlockPrefix:        1260139333,
					TransactionExtensions: []*ship.Extension{},
					Signatures: []ecc.Signature{
						ecc.MustNewSignature("SIG_K1_KiMCNgMWX9e9n5N4hA3gjwNmxiLz7f6DN3spDk2DyF32td8JfH3R7HXHpFvBfbJSFvpEGFszaMQQRvotogoeVtGq4Cp34P"),
					},
					ContextFreeData: []uint8{},
				},
			},
		},
	}

	trace_handler := func(r []*ship.TransactionTraceV0) {
		opts := cmpopts.IgnoreUnexported(ecc.Signature{})
		called = true
		assert.Equal(t, 2, len(r))
		assert.DeepEqual(t, trace0, *r[0], opts)
		assert.DeepEqual(t, trace1, *r[1], opts)
	}

	stream := NewStream(WithStartBlock(279028468), WithTraceHandler(trace_handler))

	block0, err := loadBlock("testdata/antelope_bock279028468.hex")
	assert.NilError(t, err)

	handler := testHandler{
		t:                    t,
		ExpectedBlockRequest: stream.blockRequest(),
		responses: [][]byte{
			block0,
		},
	}

	s := newServerWithHandler(t, &handler)
	defer s.Close()

	err = stream.Connect(s.URL.String())
	assert.NilError(t, err)
	err = stream.SendBlocksRequest()
	assert.NilError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		_ = stream.Shutdown()
	}()

	err = stream.Run()
	assert.Error(t, err, "websocket: close 1000 (normal)")

	assert.Assert(t, called, "Trace callback never called")
}
