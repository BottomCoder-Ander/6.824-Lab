package rpc

type SumRequest struct {
	A int
	B int
}

type SumReply struct {
	Result int
}
