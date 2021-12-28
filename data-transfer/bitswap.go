package main

import (
	"context"
	"fmt"
	"io"
	gsync "sync"
	"time"

	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"

	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	dtbs "github.com/ipfs/test-plans/data-transfer/bitswap"
	//blockservice "github.com/ipfs/test-plans/data-transfer/bitswap/bservice"
	merkledag "github.com/ipfs/test-plans/data-transfer/bitswap/mdag"

	mdagorig "github.com/ipfs/go-merkledag"
)

var rootCidTopic = sync.NewTopic("rootCid", new(cid.Cid))
var doneState = sync.State("test-done")

func bitswap1to1(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for k, v := range runenv.TestInstanceParams {
		runenv.RecordMessage("key: %s, value: %s", k, v)
	}

	ti, err := setupBaseLibp2pTest(ctx, runenv, initCtx)
	if err != nil {
		return err
	}

	runenv.RecordMessage("start test")

	switch ti.seq {
	case 1:
		if err := bitswapServer(ctx, runenv, initCtx, ti); err != nil {
			return err
		}
	case 2:
		if err := bitswapClient(ctx, runenv, initCtx, ti); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unsupported seq %d", ti.seq))
	}
	return nil
}

func bitswapServer(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo) error {
	bs, rootCid, err := GetBlockstoreFromConfig(ctx, runenv)
	if err != nil {
		return err
	}

	var bsServer io.Closer
	serverType := runenv.StringParam("servertype")
	switch serverType {
	case "standard":
		nilRouter, err := nilrouting.ConstructNilRouting(nil, nil, nil, nil)
		if err != nil {
			return err
		}
		bsopts := []bitswap.Option{bitswap.MaxOutstandingBytesPerPeer( 1 << 30)}

		bsServer = bitswap.New(ctx, network.NewFromIpfsHost(ti.h, nilRouter), bs, bsopts...)
	case "simplified":
		bsServer = dtbs.NewServer(ti.h, bs, runenv)
	default:
		panic(fmt.Sprintf("unsupported servertype %s", serverType))

	}

	initCtx.SyncClient.MustPublish(ctx, rootCidTopic, rootCid)
	defer bsServer.Close()

	runenv.RecordMessage("Published rootCID %v", rootCid)

	initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)

	return nil
}

func bitswapClient(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo) error {
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

	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return err
	}
	bstore := &CountingBS{ Blockstore: blockstore.NewBlockstore(ds), check: make(map[cid.Cid]struct{}), re: runenv}
	//bsclient := bitswap.New(ctx, network.NewFromIpfsHost(ti.h, rhelp.Null{}), bstore, bitswap.MaxOutstandingBytesPerPeer(1 << 30))
	bsclient := dtbs.NewClient(ti.h, bstore, ai1.ID, runenv)
	merkledag.RunEnv = runenv
	//blockservice.RunEnv = runenv
	dserv := mdagorig.NewDAGService(blockservice.New(bstore, bsclient))
	ctxDsrv := mdagorig.NewReadOnlyDagService(mdagorig.NewSession(ctx, dserv))

	runenv.RecordMessage("Client connect to server")

	if err := ti.h.Connect(ctx, ai1); err != nil {
		return err
	}

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

	initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)
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