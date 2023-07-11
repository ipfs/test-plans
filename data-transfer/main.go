// Welcome, testground plan writer!
// If you are seeing this for the first time, check out our documentation!
// https://app.gitbook.com/@protocol-labs/s/testground/

package main

import (
	"github.com/ipfs/go-log/v2"
	"github.com/testground/sdk-go/run"
)

var logger log.StandardLogger = log.Logger("replace-me")

var testcases = map[string]interface{}{
	"tcp":    run.InitializedTestCaseFn(tcptest),
	"libp2p": run.InitializedTestCaseFn(libp2pTest),
	"bitswap" : run.InitializedTestCaseFn(bitswap1to1),
	"graphsync" : run.InitializedTestCaseFn(graphsync1to1),
}

func main() {
	run.InvokeMap(testcases)
}
