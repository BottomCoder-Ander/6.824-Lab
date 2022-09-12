package rpc

import (
	"fmt"
	"log"
	"net/rpc"
)

func main() {
	client, err := rpc.DialHTTP("tcp", ":1234")

	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()

	sumRequest := &SumRequest{
		A: 1,
		B: 2,
	}
	sumReply := &SumReply{}
	err = client.Call("SumService.Sum", sumRequest, sumReply)

	if err != nil {
		log.Fatal("call error:", err)
	}

	fmt.Println("Result:", sumReply.Result)

}
