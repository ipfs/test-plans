// Welcome, testground plan writer!
// If you are seeing this for the first time, check out our documentation!
// https://app.gitbook.com/@protocol-labs/s/testground/

package main

import (
	"github.com/testground/sdk-go/run"
)

var testcases = map[string]interface{}{
	"tcp":    run.InitializedTestCaseFn(tcptest),
	"libp2p": run.InitializedTestCaseFn(libp2pTest),
	"bitswap" : run.InitializedTestCaseFn(bitswap1to1),
}

func main() {
	run.InvokeMap(testcases)
}
