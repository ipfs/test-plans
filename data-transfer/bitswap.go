package main

import (
	"context"
	"fmt"
	"math/rand"
	gsync "sync"
	"time"

	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
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

		ds, err := leveldb.NewDatastore("", nil)
		if err != nil {
			return err
		}
		bstore := &CountingBS{ Blockstore: blockstore.NewBlockstore(ds), check: make(map[cid.Cid]struct{}), re: runenv}
		bswap := bitswap.New(ctx, network.NewFromIpfsHost(ti.h, rhelp.Null{}), bstore, bitswap.MaxOutstandingBytesPerPeer(1 << 30))
		dserv := merkledag.NewDAGService(blockservice.New(bstore, bswap))
		ctxDsrv := merkledag.NewReadOnlyDagService(merkledag.NewSession(ctx, dserv))

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
		go func() {
			tc := time.NewTicker(time.Second * 1)
			for  {
				select {
				case <- ctx.Done():
					return
				case <- tc.C:
					runenv.RecordMessage("progress : %d", pt.Value())
				}
			}
		}()

		visitProgress := func(c cid.Cid) bool {
			if set.Visit(c) {
				pt.Increment()
				return true
			}
			return false
		}

		if err := merkledag.Walk(ctx, merkledag.GetLinksDirect(ctxDsrv), rootCid, visitProgress, merkledag.Concurrency(500)); err != nil {
			return err
		}
		runenv.RecordMessage("progress : %d", pt.Value())
		runenv.RecordMessage("progress blocks: %d", len(bstore.check))
		runenv.RecordMessage("Client finished: %s", time.Since(start))
	default:
		return fmt.Errorf("expected at most two test instances")
	}

	state := sync.State("test-done")
	initCtx.SyncClient.MustSignalAndWait(ctx, state, runenv.TestInstanceCount)

	return nil
}

type CountingBS struct {
	blockstore.Blockstore
	lm gsync.Mutex
	check map[cid.Cid]struct{}
	re *runtime.RunEnv
}

func (b *CountingBS) Put(ctx context.Context, block blocks.Block) error {
	if err := b.Blockstore.Put(ctx, block); err != nil {
		return err
	}
	b.lm.Lock()
	defer b.lm.Unlock()
	b.check[block.Cid()] = struct{}{}
	b.re.RecordMessage("put single: progress blocks : %d", len(b.check))
	return nil
}

func (b *CountingBS) PutMany(ctx context.Context, blocks []blocks.Block) error {
	if err := b.Blockstore.PutMany(ctx, blocks); err != nil {
		return err
	}
	b.lm.Lock()
	defer b.lm.Unlock()
	for _, block := range blocks {
		b.check[block.Cid()] = struct{}{}
	}
	b.re.RecordMessage("put many %d: progress blocks : %d", len(blocks), len(b.check))
	return nil
}

var _ blockstore.Blockstore = (*CountingBS)(nil)