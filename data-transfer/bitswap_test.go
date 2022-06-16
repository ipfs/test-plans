package main

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	dtbs "github.com/ipfs/test-plans/data-transfer/bitswap"
	merkledag "github.com/ipfs/test-plans/data-transfer/bitswap/mdag"
	"github.com/ipfs/test-plans/data-transfer/manifetch"
	"github.com/libp2p/go-libp2p-core/peer"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	"github.com/stretchr/testify/require"
)

func TestManifetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup server
	hs, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	t.Cleanup(func() { hs.Close() })

	const approxBlockSize = 1 << 14
	const depth = 400
	f, err := GenerateDeepDagCarFile(ctx, rand.New(rand.NewSource(0)), int(approxBlockSize), depth)
	require.NoError(t, err)

	robs, err := LoadCARBlockstore(f)
	require.NoError(t, err)

	allRoots, err := robs.Roots()
	require.NoError(t, err)
	require.Len(t, allRoots, 1)

	rootCid := allRoots[0]

	slowBS := &DelayedBlockstore{
		Blockstore: robs,
		delay:      delay.Fixed(time.Second),
	}

	_ = dtbs.NewServer(hs, slowBS, nil)

	// Setup client
	hc, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	t.Cleanup(func() { hc.Close() })

	err = hc.Connect(ctx, peer.AddrInfo{
		ID:    hs.ID(),
		Addrs: hs.Addrs(),
	})
	require.NoError(t, err)

	mss := manifetch.NewServer(slowBS, hc)

	err = mss.Start()
	require.NoError(t, err)

	msc := manifetch.NewClient(hs)

	treeIter, err := msc.Get(ctx, hc.ID(), rootCid)
	require.NoError(t, err)

	ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
	require.NoError(t, err)

	bstore := &CountingBS{Blockstore: blockstore.NewBlockstore(ds), check: make(map[cid.Cid]struct{})}
	bsclient := dtbs.NewClient(hc, hs.ID(), logger)
	pt := &merkledag.ProgressTracker{}

	// needed to avoid panic
	merkledag.Logger = logger

	err = merkledag.Walk2(ctx, bstore, bsclient, rootCid, treeIter, pt, logger, merkledag.Concurrency(1))
	require.NoError(t, err)
}
