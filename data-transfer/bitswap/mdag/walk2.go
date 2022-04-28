package merkledag

import (
	"context"
	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
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
	nd, err := m.primary.Get(ctx, c)
	if err == nil {
		return nd, nil
	}
	if err != bserv.ErrNotFound {
		return nil, err
	}
	return m.secondary.Get(ctx, c)
}

func (m *multing) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	panic("implement me")
}

var _ format.NodeGetter = (*multing)(nil)

/*
func WalkWithManifest(ctx context.Context, root cid.Cid, dserv format.DAGService, concurrency int, manifestCids <-chan cid.Cid) error {
	var visitlk sync.Mutex
	visitSet := cid.NewSet()
	visit := visitSet.Visit

	// Setup synchronization
	grp, errGrpCtx := errgroup.WithContext(ctx)

	cacheDstore, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return err
	}
	cacheBstore := blockstore.NewBlockstore(cacheDstore)
	cacheDserv := &multing{
		primary: NewDAGService(bserv.New(cacheBstore, offline.Exchange(cacheBstore)))),
		secondary: dserv,
	}
	getLinks := GetLinksDirect(cacheDserv)

		Plan:
		1. Setup X goroutines for parallelism
		2. Seed taskqueue with the root CID
		3. As new CIDs are discovered add to taskqueue
			a. Also do updates to manifest queue including:
				1) Removing CIDs from the manifest queue that are from pending
				2) Giving credit to the manifest when a CID that was downloaded because of it is added to the pending queue
		4. As manifest CIDs come in add to keep a buffer of size X outstanding CIDs

		... Manifest CIDs could be bogus, perhaps after X seconds cancel them and recombine into a GetMany with O(1) goroutines

	var manifestLk sync.RWMutex
	manifestMaxCredit := 1
	manifestCurrentCredit := 1
	manifestOutstandingMHs := make(map[string]struct{})

	// Input and output queues for workers.
	feed := make(chan *listCids)
	out := make(chan *listCids)
	done := make(chan struct{})

	grp.Go(func() error {
		for {

		}
	})

	for i := 0; i < concurrency; i++ {
		grp.Go(func() error {
			for feedChildren := range feed {
				var linksToVisit []cid.Cid
				for _, nextCid := range feedChildren.cids {
					var shouldVisit bool

					visitlk.Lock()
					shouldVisit = visit(nextCid)
					visitlk.Unlock()

					if shouldVisit {
						linksToVisit = append(linksToVisit, nextCid)
					}
				}

				getLinks(errGrpCtx)

				chNodes := dserv.GetMany(errGrpCtx, linksToVisit)
				for optNode := range chNodes {
					if optNode.Err != nil {
						return optNode.Err
					}

					nextShard, err := NewHamtFromDag(dserv, optNode.Node)
					if err != nil {
						return err
					}

					nextChildren, err := nextShard.walkChildren(processShardValues)
					if err != nil {
						return err
					}

					select {
					case out <- nextChildren:
					case <-errGrpCtx.Done():
						return nil
					}
				}

				select {
				case done <- struct{}{}:
				case <-errGrpCtx.Done():
				}
			}
			return nil
		})
	}

	send := feed
	var todoQueue []*listCids
	var inProgress int

	next := &listCids{
		cids: []cid.Cid{root},
	}

dispatcherLoop:
	for {
		select {
		case send <- next:
			inProgress++
			if len(todoQueue) > 0 {
				next = todoQueue[0]
				todoQueue = todoQueue[1:]
			} else {
				next = nil
				send = nil
			}
		case <-done:
			inProgress--
			if inProgress == 0 && next == nil {
				break dispatcherLoop
			}
		case nextNodes := <-out:
			if next == nil {
				next = nextNodes
				send = feed
			} else {
				todoQueue = append(todoQueue, nextNodes)
			}
			for _, c := range nextNodes.cids {
				k := string(c.Hash())
				if _, ok := manifestOutstandingMHs[k]; ok {
					delete(manifestOutstandingMHs, k)
					manifestMaxCredit++
				}
			}
		case <-errGrpCtx.Done():
			break dispatcherLoop
		default:
			select {
			case mcid := <-manifestCids:

			}
		}
	}
	close(feed)
	return grp.Wait()
}
*/

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
	cacheDserv := &multing{
		primary:   NewDAGService(bserv.New(cacheBstore, client)),
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
	const startingCredit = 1
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
		for c := range manifestCids {
			manifestLk.Lock()
			gotCredit := false
			if manifestCurrentCredit > 0 {
				manifestCurrentCredit--
				gotCredit = true
			}
			manifestLk.Unlock()
			if !gotCredit {
				<-manifestCreditAvailable
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
	}()

	go func() {
		i := 0
		for b := range blks {
			if err := cacheBstore.Put(ctx, b); err != nil {
				panic(err)
			}
			i++
			logger.Debugf("found manifest blocks number %d, cid %s", i, b.Cid())
		}
	}()

	return Walk(ctx, getLinks, rootCid, visitProgress, options...)
}
