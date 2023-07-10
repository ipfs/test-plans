package bitswap

import (
	"context"
	"fmt"
	"github.com/testground/sdk-go/runtime"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var (
	ProtocolBitswap        protocol.ID = "/ipfs/bitswap/1.2.0"
)

var _ exchange.Fetcher = (*Client)(nil)
var _ exchange.SessionExchange = (*Client)(nil)

type response struct {
	blk blocks.Block
}

type Client struct {
	Host          host.Host
	Blockstore    blockstore.Blockstore

	wantsMut sync.RWMutex
	// map from (cid,peer) -> requestid -> response chan
	wants map[string]map[string]chan response

	peer peer.ID
	stream network.Stream

	re *runtime.RunEnv
}

func (c *Client) NewSession(ctx context.Context) exchange.Fetcher {
	return c
}

func (c *Client) HasBlock(ctx context.Context, block blocks.Block) error {
	c.re.RecordMessage("HasBlock: %v", block.Cid())
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
		str, err := c.Host.NewStream(ctx, c.peer, ProtocolBitswap)
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

	c.re.RecordMessage("Ready to send WANTS for %d CIDs", len(cids))
	// write the request
	err := msg.ToNetV1(c.stream)
	if err != nil {
		// TODO: this is a problematic case currently, if there's an error we might be leaking channels into the map
		panic(err)
		return nil, err
	}
	c.re.RecordMessage("Sent WANTS for %d CIDs", len(cids))

	return out, nil
}

func wantsKey(cid cid.Cid, peerID peer.ID) string {
	return fmt.Sprintf("%s,%s", cid.String(), peerID.String())
}

func (c *Client) handleStream(stream network.Stream) {
	defer func() {
		if err := stream.Close(); err != nil {
			log.Print("error closing stream")
		}
	}()
	respMsg, err := message.FromNet(stream)
	if err != nil {
		log.Printf("error reading message from stream, discarding: %s", err.Error())
	}
	c.wantsMut.Lock()
	defer c.wantsMut.Unlock()
	toDelete := map[string]string{}
	for _, dontHave := range respMsg.DontHaves() {
		k := wantsKey(dontHave, stream.Conn().RemotePeer())
		if chanMap, ok := c.wants[k]; ok {
			for reqID, ch := range chanMap {
				ch <- response{}
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
				c.re.RecordMessage("Received block %v", blk.Cid())
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
	blkResp := <-blkCh[0]
	if err := c.Blockstore.Put(ctx, blkResp.blk); err != nil {
		panic(err)
	}

	return blkResp.blk, nil
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
			if err := c.Blockstore.Put(ctx, blkResp.blk); err != nil {
				panic(err)
			}
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

func NewClient(host host.Host, blockstore blockstore.Blockstore, p peer.ID, re *runtime.RunEnv) *Client {
	c := &Client{
		Host:          host,
		Blockstore:    blockstore,
		wants:         map[string]map[string]chan response{},
		peer: p,
		re: re,
	}
	c.Host.SetStreamHandler(ProtocolBitswap, c.handleStream)
	return c
}