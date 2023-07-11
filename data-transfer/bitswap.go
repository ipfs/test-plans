package main

import (
	"context"
	"fmt"
	"io"
	gr "runtime"
	gsync "sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/test-plans/data-transfer/manifetch"
	"github.com/ipfs/test-plans/data-transfer/tglog"
	"github.com/libp2p/go-libp2p-core/peer"

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

	logger = &tglog.RunenvLogger{Re: runenv}

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
		nilRouter, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
		if err != nil {
			return err
		}
		bsopts := []bitswap.Option{bitswap.MaxOutstandingBytesPerPeer(1 << 30)}

		bsServer = bitswap.New(ctx, network.NewFromIpfsHost(ti.h, nilRouter), bs, bsopts...)
	case "simplified":
		bsServer = dtbs.NewServer(ti.h, bs, runenv)
	default:
		panic(fmt.Sprintf("unsupported servertype %s", serverType))
	}

	manifestFetchType := runenv.StringParam("manifestfetchtype")
	switch manifestFetchType {
	case "none":
	case "manifetch":
		manifetchBlockLatency := runenv.StringParam("manifetchbslatency")
		dur, err := time.ParseDuration(manifetchBlockLatency)
		if err != nil {
			return err
		}

		dbs := bs.(*DelayedBlockstore)
		baseBS := dbs.Blockstore

		delayedBS := &DelayedBlockstore{
			Blockstore: baseBS,
			delay:      delay.Fixed(dur),
		}

		mserver := manifetch.NewServer(delayedBS, ti.h)

		if err := mserver.Start(); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unsupported manifestfetchtype %s", manifestFetchType))
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

	manifestFetchType := runenv.StringParam("manifestfetchtype")
	switch manifestFetchType {
	case "none":
		return basicDAGWalker(ctx, runenv, initCtx, ti, ai1, rootCid)
	case "manifetch":
		return manifetchDAGWalker(ctx, runenv, initCtx, ti, ai1, rootCid)
	default:
		panic(fmt.Sprintf("unsupported manifestfetchtype %s", manifestFetchType))
	}
}

func manifetchDAGWalker(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo, ai1 peer.AddrInfo, rootCid cid.Cid) error {
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return err
	}
	bstore := &CountingBS{Blockstore: blockstore.NewBlockstore(ds), check: make(map[cid.Cid]struct{}), re: runenv}
	bsclient := dtbs.NewClient(ti.h, ai1.ID, logger)
	merkledag.Logger = logger

	runenv.RecordMessage("Client connect to server")

	if err := ti.h.Connect(ctx, ai1); err != nil {
		return err
	}

	runenv.RecordMessage("Client starting download")
	start := time.Now()
	pt := &merkledag.ProgressTracker{}
	go func() {
		tc := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tc.C:
				runenv.RecordMessage("progress : %d", pt.Value())
			}
		}
	}()

	mcli := manifetch.NewClient(ti.h)

	iter, err := mcli.Get(ctx, ai1.ID, rootCid)
	if err != nil {
		return err
	}

	go func() {
		<-time.After(time.Second * 15)
		buf := make([]byte, 1024*1024*10)
		size := gr.Stack(buf, true)
		runenv.RecordMessage("stackdump size", size)
		panic(string(buf[:size]))
	}()

	if err := merkledag.Walk2(ctx, bstore, bsclient, rootCid, iter, pt, logger, merkledag.Concurrency(1)); err != nil {
		panic(err)
	}

	runenv.RecordMessage("progress : %d", pt.Value())
	runenv.RecordMessage("progress blocks: %d", len(bstore.check))
	runenv.RecordMessage("Client finished: %s", time.Since(start))

	initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)
	return nil
}

func basicDAGWalker(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo, ai1 peer.AddrInfo, rootCid cid.Cid) error {
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return err
	}
	bstore := &CountingBS{Blockstore: blockstore.NewBlockstore(ds), check: make(map[cid.Cid]struct{}), re: runenv}
	bsclient := dtbs.NewClient(ti.h, ai1.ID, logger)
	merkledag.Logger = logger
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
		for {
			select {
			case <-ctx.Done():
				return
			case <-tc.C:
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
	lm    gsync.Mutex
	check map[cid.Cid]struct{}
	re    *runtime.RunEnv
}

func (b *CountingBS) Put(ctx context.Context, block blocks.Block) error {
	if err := b.Blockstore.Put(ctx, block); err != nil {
		return err
	}
	b.lm.Lock()
	defer b.lm.Unlock()
	b.check[block.Cid()] = struct{}{}
	if b.re != nil {
		b.re.RecordMessage("put single: progress blocks : %d", len(b.check))
	} else {
		fmt.Printf("put single: progress blocks : %d\n", len(b.check))
	}
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
	if b.re != nil {
		b.re.RecordMessage("put many %d: progress blocks : %d", len(blocks), len(b.check))
	} else {
		fmt.Printf("put many %d: progress blocks : %d\n", len(blocks), len(b.check))
	}
	return nil
}

var _ blockstore.Blockstore = (*CountingBS)(nil)
