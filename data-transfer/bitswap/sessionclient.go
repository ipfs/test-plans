package bitswap

/*
import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/testground/sdk-go/runtime"
	"log"
	"sync"
)

type session struct {
	mx     sync.RWMutex
	cs     *cid.Set
	respCh chan blocks.Block
	peers  map[peer.ID]struct{}
}

type SessionClient struct {
	Host host.Host

	mx sync.RWMutex
	// map from mh -> sessions requesting mh
	sessions map[string][]session

	// outstanding wants by CID
	wants map[string]map[peer.ID]struct{}

	peerStreams map[peer.ID]network.Stream

	re *runtime.RunEnv
}

func (c *SessionClient) NewSession() *session {
	return &session{
		cs:     cid.NewSet(),
		respCh: make(chan blocks.Block),
		peers:  make(map[peer.ID]struct{}),
	}
}

func (c *SessionClient) AddPeerToSession(ctx context.Context, s *session, p peer.ID) error {
	c.mx.RLock()
	stream, found := c.peerStreams[p]
	c.mx.RUnlock()
	var err error

	if !found {
		stream, err = c.Host.NewStream(ctx, p, ProtocolBitswap)
		if err != nil {
			return err
		}
		c.mx.Lock()
		c.peerStreams[p] = stream
		c.mx.Unlock()
	}

	s.mx.Lock()
	if _, found := s.peers[p]; found {
		return nil
	}
	s.peers[p] = struct{}{}
	s.mx.Unlock()
	s.mx.RLock()
	err = s.cs.ForEach(func(c cid.Cid) error {

	})
	if err != nil {
		s.mx.RUnlock()
		return err
	}
	s.mx.RUnlock()

	return nil
	stream, err := c.Host.NewStream(ctx, p)
	if err != nil {
		return err
	}
	c.mx.Lock()
}


func (c *SessionClient) getBlockFromPeer(ctx context.Context, cids []cid.Cid) ([]<-chan response, error) {
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

func (c *SessionClient) handleStream(stream network.Stream) {
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

func (c *SessionClient) GetBlock(ctx context.Context, blkCid cid.Cid) (blocks.Block, error) {
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

func (c *SessionClient) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
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

func NewSessionClient(host host.Host, re *runtime.RunEnv) *SessionClient {
	c := &SessionClient{
		Host:        host,
		sessions:    make(map[string][]session),
		wants:       make(map[string]map[peer.ID]struct{}),
		peerStreams: make(map[peer.ID]network.Stream),
		re:          re,
	}
	c.Host.SetStreamHandler(ProtocolBitswap, c.handleStream)
	return c
}
*/
