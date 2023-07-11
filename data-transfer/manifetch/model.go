package manifetch

// Request will be sent when a peer wants CIDs reachable from Root
type Request struct {
	// Root is a CID in a string format.
	Root string
	// Headers are custom key/value pairs that can be used for special client/server communication (filters, selectors, special requests...).
	// If the server doesn't know about a specific header, it will be ignored.
	// The client will know when a header is ignored from the headers in the response.
	// The client must handle the cases when a request header is ignored, and make all the needed work to obtain data as it was expected.
	Headers map[string]string
}

// Response will be sent from the server when a request is received.
// After a response, the server will send from 0 to N Node objects with all the requested data.
type Response struct {
	// Headers are a custom key/value pairs that can be used for special client/server communication.
	// These headers are headers expected after headers sent on the request.
	// If the server didn't know about some of the headers sent on the request,
	// they will be ignored, so the expect response header won't appear here.
	Headers map[string]string
}

// TODO maybe send several CIDs at once in one message to make it more performant?
// TODO maybe add headers here too?

// Node is a result sent after a response. It contains information about blocks stored by the server peer.
// It contains the CID in a string format, and the size of the block.
type Node struct {
	CID  string
	Size int64
}
