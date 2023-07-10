package bitswap

import (
	"context"
	"io"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"

	"github.com/testground/sdk-go/runtime"
)

type Server struct {
	Host host.Host
	Blockstore blockstore.Blockstore
	RunEnv *runtime.RunEnv
}

func NewServer(host host.Host, blockstore blockstore.Blockstore, re *runtime.RunEnv) *Server {
	s := &Server{
		Host:          host,
		Blockstore:    blockstore,
		RunEnv: re,
	}
	s.Host.SetStreamHandler(ProtocolBitswap, s.handle)
	return s
}

func (s *Server) Close() error {
	return nil
}

func (s *Server) handle(stream network.Stream) {
	defer stream.Close()

	reader := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
	for {
		received, err := bsmsg.FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				_ = stream.Reset()
			}
			return
		}

		p := stream.Conn().RemotePeer()
		wl := received.Wantlist()

		s.RunEnv.RecordMessage("Server received message with wantlist size %d", len(wl))
		for i := range wl {
			e := wl[i]
			if e.Cancel == true {
				panic("unsupported cancel")
			}
			if e.SendDontHave == false {
				panic("unsupported senddonthave")
			}

			go func() {
				msg := bsmsg.New(false)

				if e.WantType == pb.Message_Wantlist_Block {
					blk, err := s.Blockstore.Get(context.TODO(), e.Cid)
					if err != nil {
						panic(err)
						return
					}
					msg.AddBlock(blk)
				} else {
					found, err := s.Blockstore.Has(context.TODO(), e.Cid)
					if err != nil {
						panic(err)
						return
					}
					if found {
						msg.AddHave(e.Cid)
					} else {
						msg.AddDontHave(e.Cid)
					}
				}

				s.RunEnv.RecordMessage("Server new stream for sending block %v", e.Cid)
				newStream, err := s.Host.NewStream(context.Background(), p, ProtocolBitswap)
				if err != nil {
					panic(err)
				}
				s.RunEnv.RecordMessage("Server sending block %v", e.Cid)
				err = msg.ToNetV1(newStream)
				if err != nil {
					panic(err)
				}
				s.RunEnv.RecordMessage("Server sent block %v", e.Cid)
				err = newStream.Close()
				if err != nil {
					panic(err)
				}
				s.RunEnv.RecordMessage("Server stream closed block %v", e.Cid)
			}()
		}
	}
}
