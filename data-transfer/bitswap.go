package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/ipfs/go-merkledag"
	rhelp "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/multiformats/go-multiaddr"

	"github.com/aschmahmann/vole/lib"
)

func bitswapUnixFSFileTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ti, err := setupBaseLibp2pTest(ctx, runenv, initCtx)
	if err != nil {
		return err
	}

	fileSize, err := humanize.ParseBytes("10 MB")
	if err != nil {
		return err
	}

	nilRouter, err := nilrouting.ConstructNilRouting(nil, nil, nil, nil)
	if err != nil {
		return err
	}

	rootCidTopic := sync.NewTopic("rootCid", new(cid.Cid))

	switch ti.seq {
	case 1:
		bs, err := GenerateUnixFSFileBS(ctx, rand.New(rand.NewSource(0)), fileSize)
		if err != nil {
			return err
		}

		roots, err := bs.Roots()
		if err != nil {
			return err
		}
		if len(roots) != 1 {
			return fmt.Errorf("there should be exactly one root, instead there are %d", len(roots))
		}

		bswap := bitswap.New(ctx, network.NewFromIpfsHost(ti.h, nilRouter), bs)
		initCtx.SyncClient.MustPublish(ctx, rootCidTopic, roots[0])
		defer bswap.Close()
	case 2:
		// Now subscribe to the peers topic and consume all addresses, storing them
		// in the peers slice.
		cidCh := make(chan *cid.Cid)
		sctx, scancel := context.WithCancel(ctx)
		sub := initCtx.SyncClient.MustSubscribe(sctx, rootCidTopic, cidCh)

		var rootCid cid.Cid
		select {
		case c := <-cidCh:
			runenv.RecordMessage("received CID %s", c.String())
			rootCid = *c
		case err := <-sub.Done():
			scancel()
			return err
		}
		scancel() // cancels the Subscription.

		ai1 := ti.peers[1]
		if err := ti.h.Connect(ctx, ai1); err != nil {
			return err
		}

		runenv.RecordMessage("Try single block vole check")
		conns := ti.h.Network().ConnsToPeer(ai1.ID)
		runenv.RecordMessage("Remote multiaddr: %v, ai1 : %v, numConns %d", conns[0].RemoteMultiaddr(), ai1, len(conns))

		ma, err := multiaddr.NewMultiaddr(conns[0].RemoteMultiaddr().String() + "/p2p/" + ai1.ID.String())
		if err != nil {
			return err
		}
		vo, err := vole.CheckBitswapCID(ctx, rootCid, ma)
		if err != nil {
			return err
		}
		runenv.RecordMessage("Finished vole check: %v", vo)

		ds, err := leveldb.NewDatastore("", nil)
		if err != nil {
			return err
		}
		bstore := blockstore.NewBlockstore(ds)
		bswap := bitswap.New(ctx, network.NewFromIpfsHost(ti.h, rhelp.Null{}), bstore)
		dserv := merkledag.NewDAGService(blockservice.New(bstore, bswap))
		ctxDsrv := merkledag.NewReadOnlyDagService(merkledag.NewSession(ctx, dserv))

		runenv.RecordMessage("Try single block download")
		foundBlk, err := bswap.GetBlock(ctx, rootCid)
		if err != nil {
			return err
		}
		runenv.RecordMessage("data received : %s", string(foundBlk.RawData()))

		runenv.RecordMessage("Client starting download")
		start := time.Now()
		set := cid.NewSet()
		pt := &merkledag.ProgressTracker{}
		ptCtx := pt.DeriveContext(ctx)
		go func() {
			tc := time.NewTicker(time.Second * 10)
			for  {
				select {
				case <- ctx.Done():
					return
				case <- tc.C:
					for _, p := range ti.h.Network().Peers() {
						protos, err := ti.h.Peerstore().GetProtocols(p)
						if err != nil {
							runenv.RecordMessage("peer proto err: %v", err)
						}
						runenv.RecordMessage("peer : %v, protos : %v", p, protos)
					}
					runenv.RecordMessage("progress : %d", pt.Value())
				}
			}
		}()
		if err := merkledag.Walk(ptCtx, merkledag.GetLinksDirect(ctxDsrv), rootCid, set.Visit, merkledag.Concurrency(500)); err != nil {
			return err
		}
		runenv.RecordMessage("Client finished: %s", time.Since(start))
	default:
		return fmt.Errorf("expected at most two test instances")
	}

	state := sync.State("test-done")
	initCtx.SyncClient.MustSignalAndWait(ctx, state, runenv.TestInstanceCount)

	return nil
}
