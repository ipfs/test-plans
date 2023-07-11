package manifetch

import (
	"context"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	mdagorig "github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"
)

const manifetchID = "/ipfs/manifetch/v1"

type Server struct {
	bs blockstore.Blockstore
	h  host.Host
}

func NewServer(bs blockstore.Blockstore, h host.Host) *Server {
	return &Server{bs: bs, h: h}
}

func (s *Server) Start() error {
	s.h.SetStreamHandler(manifetchID, s.streamHandler)
	return nil
}

func (s *Server) Stop() error {
	s.h.RemoveStreamHandler(manifetchID)
	return nil
}

func (s *Server) streamHandler(stream network.Stream) {
	defer stream.Reset()

	// TODO add default timeout to process the request
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
	msg, err := r.ReadMsg()
	if err != nil {
		// TODO log errors
		fmt.Println(err)
		return
	}

	req := &Request{}
	if err := cbor.Unmarshal(msg, req); err != nil {
		// TODO log errors
		fmt.Println(err)
		return
	}

	// TODO handle headers
	fmt.Println("HEADER REQUEST", req.Headers)

	w := msgio.NewVarintWriter(stream)
	defer w.Close()

	bdata, err := cbor.Marshal(&Response{})
	if err != nil {
		// TODO log errors
		fmt.Println(err)
		return
	}

	if err := w.WriteMsg(bdata); err != nil {
		// TODO log errors
		fmt.Println(err)
		return
	}

	// TODO change this to use a DAGStore index

	visitSet := cid.NewSet()
	visitProgress := func(c cid.Cid) bool {
		if visitSet.Visit(c) {
			// TODO remove all this logic from here
			block, err := s.bs.Get(context.TODO(), c)
			if err != nil {
				// TODO: Better handling
				fmt.Println("ERROR GETTING BLOCK", err)
				return false
			}

			ndata := &Node{CID: c.String(), Size: int64(len(block.RawData()))}
			bdata, err := cbor.Marshal(ndata)
			if err != nil {
				// TODO: Better handling
				fmt.Println("ERROR MARSHALLING DATA", err)
				return true
			}

			if err := w.WriteMsg(bdata); err != nil {
				// TODO: Better handling
				fmt.Println("ERROR WRITTING DATA", err)
			}

			return true
		}
		return false
	}

	ctxDsrv := mdagorig.NewReadOnlyDagService(mdagorig.NewSession(ctx,
		mdagorig.NewDAGService(blockservice.New(s.bs, offline.Exchange(s.bs))),
	))

	rootCid, err := cid.Decode(req.Root)
	if err != nil {
		// TODO: Better handling
		fmt.Println("ERROR DECODING CID", err)
		return
	}

	if err := mdagorig.Walk(ctx, mdagorig.GetLinksDirect(ctxDsrv), rootCid, visitProgress, mdagorig.Concurrency(500)); err != nil {
		// TODO: Better handling
		fmt.Println("ERROR WALKING DAG", err)
		return
	}
}
