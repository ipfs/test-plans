package merkledag

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-log/v2"
	mdagorig "github.com/ipfs/go-merkledag"
	"github.com/ipfs/test-plans/data-transfer/bitswap"
	blockservice "github.com/ipfs/test-plans/data-transfer/bitswap/bservice"
	"sync"
	"time"
)

type listCids struct {
	cids []cid.Cid
}

type multing struct {
	primary   format.NodeGetter
	secondary format.NodeGetter
}

func (m *multing) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	/*nd, err := m.primary.Get(ctx, c)
	if err == nil {
		return nd, nil
	}
	if err != bserv.ErrNotFound {
		return nil, err
	}
	return m.secondary.Get(ctx, c)*/

	ch := make(chan format.Node, 2)
	parallelCtx, cancel := context.WithCancel(ctx)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer cancel()
		nd, err := m.primary.Get(parallelCtx, c)
		if err == nil {
			for {
				select {
				case ch <- nd:
					return
				case <-parallelCtx.Done():
					return
				case <- time.After(time.Second *3):
					Logger.Debugf("waiting for primary CID %s", c)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer cancel()
		nd, err := m.secondary.Get(parallelCtx, c)
		if err == nil {
			for {
				select {
				case ch <- nd:
					return
				case <-parallelCtx.Done():
					return
				case <- time.After(time.Second *3):
					Logger.Debugf("waiting for secondary CID %s", c)
				}
			}
		}
	}()

	wg.Wait()

	select {
	case nd := <-ch:
		return nd, nil
	default:
		Logger.Debugf("abort query for CID %s", c)
		return nil, format.ErrNotFound
	}
}

func (m *multing) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	panic("implement me")
}

var _ format.NodeGetter = (*multing)(nil)

type ngwrapper struct {
	bstore blockstore.Blockstore

	waitLk    sync.Mutex
	waitMHMap map[string]map[chan struct{}]struct{}
}

func (n *ngwrapper) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	n.waitLk.Lock()
	b, err := n.bstore.Get(ctx, c)
	if err == nil {
		n.waitLk.Unlock()
		return legacy.DecodeNode(ctx, b)
	}
	if err != blockstore.ErrNotFound {
		n.waitLk.Unlock()
		return nil, err
	}
	waitCh := make(chan struct{}, 1)
	m, found := n.waitMHMap[string(c.Hash())]
	if !found {
		n.waitMHMap[string(c.Hash())] = make(map[chan struct{}]struct{})
		m = n.waitMHMap[string(c.Hash())]
	}
	m[waitCh] = struct{}{}
	n.waitLk.Unlock()
	for {
		select {
		case <-waitCh:
			b, err := n.bstore.Get(ctx, c)
			if err == nil {
				return legacy.DecodeNode(ctx, b)
			}
			if err == blockstore.ErrNotFound {
				return nil, format.ErrNotFound
			}
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second * 3):
			f, err := n.bstore.Has(ctx, c)
			Logger.Debugf("stuck waiting for %s: Has %v, Err %v", c, f, err)
		}
	}
}

func (n *ngwrapper) Alert(c cid.Cid) {
	n.waitLk.Lock()
	defer n.waitLk.Unlock()
	chMap, found := n.waitMHMap[string(c.Hash())]
	if !found {
		Logger.Debugf("alert notfound %s", c)
		return
	}
	Logger.Debugf("alert found %s, mapsize", c, len(chMap))
	for ch := range chMap {
		ch <- struct{}{}
		close(ch)
	}
	delete(n.waitMHMap, string(c.Hash()))
}

func (n *ngwrapper) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	//TODO implement me
	panic("implement me")
}

var _ format.NodeGetter = (*ngwrapper)(nil)

func Walk2(ctx context.Context, bstore blockstore.Blockstore, client *bitswap.Client,
	rootCid cid.Cid, manifestCids <-chan cid.Cid, pt *ProgressTracker, logger log.StandardLogger, options ...WalkOption) error {
	/*cacheDstore, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return err
	}*/
	cacheDstore := ds_sync.MutexWrap(datastore.NewMapDatastore())

	dservBase := mdagorig.NewDAGService(blockservice.New(bstore, client))
	dserv := mdagorig.NewReadOnlyDagService(mdagorig.NewSession(ctx, dservBase))

	cacheBstore := blockstore.NewBlockstore(cacheDstore)
	cacheNG := &ngwrapper{
		bstore:    cacheBstore,
		waitMHMap: make(map[string]map[chan struct{}]struct{}),
	}
	cacheDserv := &multing{
		primary:   cacheNG,
		secondary: dserv,
	}
	getLinks := GetLinksDirect(cacheDserv)

	var cslk sync.RWMutex
	cset := cid.NewSet()

	/*
		Plan:
		1. Setup X goroutines for parallelism
		2. Seed taskqueue with the root CID
		3. As new CIDs are discovered add to taskqueue
			a. Also do updates to manifest queue including:
				1) Removing CIDs from the manifest queue that are from pending
				2) Giving credit to the manifest when a CID that was downloaded because of it is added to the pending queue
		4. As manifest CIDs come in add to keep a buffer of size X outstanding CIDs

		... Manifest CIDs could be bogus, perhaps after X seconds cancel them and recombine into a GetMany with O(1) goroutines
	*/

	var manifestLk sync.RWMutex
	const startingCredit = 100
	manifestMaxCredit := startingCredit
	manifestCurrentCredit := manifestMaxCredit
	manifestOutstandingMHs := make(map[string]struct{})
	manifestCreditAvailable := make(chan struct{})

	go func() {
		tc := time.NewTicker(time.Second)
		for {
			select {
			case <-tc.C:
				manifestLk.RLock()
				logger.Debugf("max credit %d, current credit %d", manifestMaxCredit, manifestCurrentCredit)
				manifestLk.RUnlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Debugf("start from root %s", rootCid)
	visitProgress := func(c cid.Cid) bool {
		cslk.Lock()
		visit := cset.Visit(c)
		if visit {
			logger.Debugf("cidvisit set %d, cid %s", cset.Len(), c)
		}
		cslk.Unlock()
		if visit {
			manifestLk.Lock()
			h := string(c.Hash())
			_, found := manifestOutstandingMHs[h]
			if found {
				delete(manifestOutstandingMHs, h)
				manifestMaxCredit += 1
				manifestCurrentCredit += 2
				select {
				case manifestCreditAvailable <- struct{}{}:
				default:
				}
			}
			manifestLk.Unlock()
			/*
				if found {
					blk, err := cacheBstore.Get(ctx, c)
					if err == nil {
						if err := bstore.Put(ctx, blk); err != nil {
							panic(err)
						}
						if err := cacheBstore.DeleteBlock(ctx, c); err != nil {

						}
					}
				}
			*/
			pt.Increment()
			return true
		}
		return false
	}

	reqCids := make(chan cid.Cid)
	blks, err := client.GetBlocksCh(ctx, reqCids)
	if err != nil {
		return err
	}

	go func() {
		defer close(reqCids)
		i := 0
		for c := range manifestCids {
			i++
			logger.Debugf("manifest CID ready to request %d", i)
			manifestLk.Lock()
			gotCredit := false
			if manifestCurrentCredit > 0 {
				manifestCurrentCredit--
				gotCredit = true
			}
			manifestLk.Unlock()
			if !gotCredit {
			foo:
				for {
					select {
					case <-manifestCreditAvailable:
						break foo
					case <-time.After(time.Second * 3):
						Logger.Debugf("waiting for credit")
					}
				}
				manifestLk.Lock()
				if manifestCurrentCredit > 0 {
					manifestCurrentCredit--
				} else {
					panic("how!")
				}
				manifestLk.Unlock()
			}

			select {
			case reqCids <- c:
				logger.Debugf("manifest CIDs requested %d", i)
				cslk.RLock()
				manifestLk.Lock()
				if !cset.Has(c) {
					manifestOutstandingMHs[string(c.Hash())] = struct{}{}
				} else {
					manifestCurrentCredit++
					select {
					case manifestCreditAvailable <- struct{}{}:
					default:
					}
				}
				manifestLk.Unlock()
				cslk.RUnlock()
			case <-ctx.Done():
				return
			}
		}
		logger.Debugf("done requesting all manifest blocks")
	}()

	go func() {
		i := 0
		for b := range blks {
			if err := cacheBstore.Put(ctx, b); err != nil {
				panic(err)
			}
			cacheNG.Alert(b.Cid())
			i++
			logger.Debugf("found manifest blocks number %d, cid %s", i, b.Cid())
		}
		logger.Debugf("found all %d manifest blocks", i)
	}()

	return Walk(ctx, getLinks, rootCid, visitProgress, options...)
}
