package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-graphsync"
	"github.com/libp2p/go-libp2p-core/peer"

	"io"
	"time"

	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"

	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

func graphsync1to1(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
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
		if err := gsServer(ctx, runenv, initCtx, ti); err != nil {
			return err
		}
	case 2:
		if err := gsClient(ctx, runenv, initCtx, ti); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unsupported seq %d", ti.seq))
	}
	return nil
}

func gsServer(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo) error {
	bs, rootCid, err := GetBlockstoreFromConfig(ctx, runenv)
	if err != nil {
		return err
	}

	ls := cidlink.LinkSystemUsingMulticodecRegistry(multicodec.DefaultRegistry)
	ls.StorageReadOpener = func(linkContext linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		blk, err := bs.Get(linkContext.Ctx, link.(cidlink.Link).Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}

	gs := gsimpl.New(ctx, gsnet.NewFromLibp2pHost(ti.h), ls)
	gs.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		hookActions.ValidateRequest()
	})

	initCtx.SyncClient.MustPublish(ctx, rootCidTopic, rootCid)

	runenv.RecordMessage("Published rootCID %v", rootCid)

	initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)

	return nil
}

func gsClient(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, ti *Libp2pTestInfo) error {
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

	ls := storeutil.LinkSystemForBlockstore(bstore)
	gs := gsimpl.New(ctx, gsnet.NewFromLibp2pHost(ti.h), ls)
	_ = gs

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	matchAllSelector := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Node()

	runenv.RecordMessage("Client connect to server")

	if err := ti.h.Connect(ctx, ai1); err != nil {
		return err
	}

	runenv.RecordMessage("Client starting download")
	start := time.Now()

	respCh, errCh := gs.Request(ctx, ai1.ID, cidlink.Link{Cid: rootCid}, matchAllSelector)
	for range respCh {
	}
	for err := range errCh {
		return err
	}

	runenv.RecordMessage("progress blocks: %d", len(bstore.check))
	runenv.RecordMessage("Client finished: %s", time.Since(start))

	initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)
	return nil
}