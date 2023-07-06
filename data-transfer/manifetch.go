package main

import (
	"context"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	mdagorig "github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"
)

const manifetchID = "/ipfs/manifetch/1.0.0"

func NewManifetchServer(bstore blockstore.Blockstore) (network.StreamHandler, error) {
	return func(stream network.Stream) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
		msg, err := r.ReadMsg()
		if err != nil {
			_ = stream.Reset()
			return
		}
		// TODO: Allow sending a CID and selector
		reqCid, err := cid.Cast(msg)
		if err != nil {
			_ = stream.Reset()
			return
		}

		visitSet := cid.NewSet()
		w := msgio.NewVarintWriter(stream)
		defer w.Close()

		visitProgress := func(c cid.Cid) bool {
			if visitSet.Visit(c) {
				if err := w.WriteMsg(c.Bytes()); err != nil {
					// TODO: Better handling
					panic(err)
				}
				return true
			}
			return false
		}

		ctxDsrv := mdagorig.NewReadOnlyDagService(mdagorig.NewSession(ctx,
			mdagorig.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore))),
		))

		if err := mdagorig.Walk(ctx, mdagorig.GetLinksDirect(ctxDsrv), reqCid, visitProgress, mdagorig.Concurrency(500)); err != nil {
			_ = stream.Reset()
			return
		}
	}, nil
}

func manifetchGet(stream network.Stream, root cid.Cid) (chan cid.Cid, error) {
	w := msgio.NewVarintWriter(stream)
	if err := w.WriteMsg(root.Bytes()); err != nil {
		return nil, err
	}

	if err := stream.CloseWrite(); err != nil {
		return nil, err
	}

	retCh := make(chan cid.Cid, 100)

	// TODO: Error handling
	go func() {
		defer close(retCh)
		r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
		for {
			msg, err := r.ReadMsg()
			if err != nil {
				return
			}
			c, err := cid.Cast(msg)
			if err != nil {
				return
			}
			retCh <- c
		}
	}()
	return retCh, nil
}