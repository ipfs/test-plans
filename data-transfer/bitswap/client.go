package bitswap

import (
	"context"
	"fmt"
	"sync"

	log2 "github.com/ipfs/go-log/v2"
	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var (
	ProtocolBitswap protocol.ID = "/ipfs/bitswap/1.2.0"
)

var _ exchange.Fetcher = (*Client)(nil)
var _ exchange.SessionExchange = (*Client)(nil)

type response struct {
	blk blocks.Block
}

type Client struct {
	Host host.Host

	wantsMut sync.RWMutex
	// map from (cid,peer) -> requestid -> response chan
	wants map[string]map[string]chan response

	peer   peer.ID
	stream network.Stream

	re log2.StandardLogger
}

func (c *Client) NewSession(ctx context.Context) exchange.Fetcher {
	return c
}

func (c *Client) HasBlock(ctx context.Context, block blocks.Block) error {
	logger.Debugf("HasBlock: %v", block.Cid())
	return nil
}

func (c *Client) IsOnline() bool {
	return true
}

func (c *Client) Close() error {
	return c.stream.Close()
}

func (c *Client) getBlockFromPeer(ctx context.Context, cids []cid.Cid) ([]<-chan response, error) {
	if c.stream == nil {
		str, err := c.Host.NewStream(context.TODO(), c.peer, ProtocolBitswap)
		if err != nil {
			return nil, err
		}
		c.stream = str
	}

	// send request
	msg := message.New(false)
	// ask for a dont-have, and assume the server returns with it
	// this may not be true in the large, but for now this is a super naive impl
	out := make([]<-chan response, 0, len(cids))
	c.wantsMut.Lock()
	for _, blkCid := range cids {
		msg.AddEntry(blkCid, 10, pb.Message_Wantlist_Block, true)

		// make the response channel and add it to the response map
		// the resp handler will remove the channel after sending a value on it
		ch := make(chan response, 1)
		reqID := uuid.New().String()
		key := wantsKey(blkCid, c.peer)
		if _, ok := c.wants[key]; !ok {
			c.wants[key] = map[string]chan response{}
		}
		c.wants[key][reqID] = ch
		out = append(out, ch)
	}
	c.wantsMut.Unlock()

	logger.Debugf("Ready to send WANTS for %d CIDs", len(cids))
	// write the request
	err := msg.ToNetV1(c.stream)
	if err != nil {
		// TODO: this is a problematic case currently, if there's an error we might be leaking channels into the map
		panic(err)
		return nil, err
	}
	logger.Debugf("Sent WANTS for %d CIDs", len(cids))

	return out, nil
}

func wantsKey(cid cid.Cid, peerID peer.ID) string {
	return fmt.Sprintf("%s,%s", cid.String(), peerID.String())
}

func (c *Client) handleStream(stream network.Stream) {
	respMsg, err := message.FromNet(stream)
	if err != nil {
		panic(fmt.Errorf("error reading message from stream, discarding: %s", err.Error()))
		return
	}

	if err := stream.Close(); err != nil {
		logger.Debugf("error closing stream")
	}

	c.wantsMut.Lock()
	defer c.wantsMut.Unlock()
	toDelete := map[string]string{}
	for _, dontHave := range respMsg.DontHaves() {
		panic("received dont have")
		k := wantsKey(dontHave, stream.Conn().RemotePeer())
		if chanMap, ok := c.wants[k]; ok {
			for reqID, ch := range chanMap {
				go func() { ch <- response{} }()
				toDelete[k] = reqID
			}
		}
	}
	for _, blk := range respMsg.Blocks() {
		k := wantsKey(blk.Cid(), stream.Conn().RemotePeer())
		if chanMap, ok := c.wants[k]; ok {
			for reqID, ch := range chanMap {
				ch <- response{blk: blk}
				toDelete[k] = reqID
				logger.Debugf("Received block %v", blk.Cid())
			}
		}
	}
	// remove channels we've sent values on
	for k, reqID := range toDelete {
		delete(c.wants[k], reqID)
	}
}

func (c *Client) GetBlock(ctx context.Context, blkCid cid.Cid) (blocks.Block, error) {
	blkCh, err := c.getBlockFromPeer(ctx, []cid.Cid{blkCid})
	if err != nil {
		return nil, err
	}
	if len(blkCh) != 1 {
		panic("how!")
	}

	select {
	case blkResp := <-blkCh[0]:
		return blkResp.blk, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) GetBlocksCh(ctx context.Context, cids <-chan cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)
	tmp := make(chan response)

	var mx sync.Mutex
	numBlocks := 0

	grp, errctx := errgroup.WithContext(ctx)
	grp.Go(
		func() error {
			for queryCid := range cids {
				mx.Lock()
				numBlocks++
				mx.Unlock()
				if err := c.getBlockFromPeer2(errctx, queryCid, tmp); err != nil {
					panic(err)
				}
			}
			return nil
		})
	grp.Go(func() error {
		defer close(out)
		numSent := 0
		for b := range tmp {
			select {
			case out <- b.blk:
				numSent++
				mx.Lock()
				if numSent == numBlocks {
					mx.Unlock()
					return nil
				}
				mx.Unlock()
			case <-errctx.Done():
				return errctx.Err()
			}
		}
		return nil
	})
	return out, nil
}

func (c *Client) getBlockFromPeer2(ctx context.Context, blkCid cid.Cid, ch chan response) error {
	if c.stream == nil {
		str, err := c.Host.NewStream(ctx, c.peer, ProtocolBitswap)
		if err != nil {
			return err
		}
		c.stream = str
	}

	// send request
	msg := message.New(false)
	// ask for a dont-have, and assume the server returns with it
	// this may not be true in the large, but for now this is a super naive impl
	c.wantsMut.Lock()
	msg.AddEntry(blkCid, 10, pb.Message_Wantlist_Block, true)

	// make the response channel and add it to the response map
	// the resp handler will remove the channel after sending a value on it
	reqID := uuid.New().String()
	key := wantsKey(blkCid, c.peer)
	if _, ok := c.wants[key]; !ok {
		c.wants[key] = map[string]chan response{}
	}
	c.wants[key][reqID] = ch
	c.wantsMut.Unlock()

	logger.Debugf("Ready to send WANTS for %d CIDs", 1)
	// write the request
	err := msg.ToNetV1(c.stream)
	if err != nil {
		// TODO: this is a problematic case currently, if there's an error we might be leaking channels into the map
		panic(err)
		return err
	}
	logger.Debugf("Sent WANTS for %d CIDs", 1)

	return nil
}

func (c *Client) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	blkCh, err := c.getBlockFromPeer(ctx, cids)
	if err != nil {
		return nil, err
	}

	out := make(chan blocks.Block, len(cids))

	wg := sync.WaitGroup{}
	wg.Add(len(cids))
	for range blkCh {
		go func() {
			blkResp := <-blkCh[0]
			out <- blkResp.blk
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out, nil
}

func NewClient(host host.Host, p peer.ID, re log2.StandardLogger) *Client {
	c := &Client{
		Host:  host,
		wants: map[string]map[string]chan response{},
		peer:  p,
	}
	logger = re
	c.Host.SetStreamHandler(ProtocolBitswap, c.handleStream)
	return c
}
