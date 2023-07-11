package bitswap

import (
	"context"
	"fmt"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/test-plans/data-transfer/tglog"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/testground/sdk-go/runtime"
	"io"
)

var logger log.StandardLogger = log.Logger("bs-simple-server")

type Server struct {
	Host       host.Host
	Blockstore blockstore.Blockstore
}

func NewServer(host host.Host, blockstore blockstore.Blockstore, re *runtime.RunEnv) *Server {
	if re != nil {
		logger = &tglog.RunenvLogger{Re: re}
	}
	s := &Server{
		Host:       host,
		Blockstore: blockstore,
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

		logger.Debugf("Server received message with wantlist size %d", len(wl))
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
					panic("what?")
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

				const maxTries = 3
				for try := 0; try < maxTries; try++ {
					if try != 0 {
						logger.Debugf("error sending block %s %s. Trying again %d", e.Cid, err.Error(), try)
					}
					err = sendSingleWLEntryMsgToPeer(s.Host, p, msg, e)
					if err == nil {
						break
					}
				}
				if err != nil {
					panic(err)
				}

				logger.Debugf("Server stream closed block %v", e.Cid)
			}()
		}
	}
}

func sendSingleWLEntryMsgToPeer(h host.Host, p peer.ID, msg bsmsg.BitSwapMessage, e bsmsg.Entry) error {
	logger.Debugf("Server new stream for sending block %v", e.Cid)
	newStream, err := h.NewStream(context.Background(), p, ProtocolBitswap)
	if err != nil {
		return fmt.Errorf("server could not open response stream error %w", err)
	}
	logger.Debugf("Server sending block %v", e.Cid)
	err = msg.ToNetV1(newStream)
	if err != nil {
		return fmt.Errorf("server stream error %s %w", e.Cid, err)
	}
	logger.Debugf("Server sent block %v", e.Cid)
	err = newStream.CloseWrite()
	if err != nil {
		return fmt.Errorf("server stream write closing error %s %w", e.Cid, err)
	}
	_, err = io.ReadAll(newStream)
	if err != nil {
		return fmt.Errorf("server stream close error %s %w", e.Cid, err)
	}
	return nil
}
