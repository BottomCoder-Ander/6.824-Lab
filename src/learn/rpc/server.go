package rpc

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type SumService struct {
}

func (sumService *SumService) Sum(sumRequest *SumRequest, sumReplay *SumReply) error {
	sumReplay.Result = sumRequest.A + sumRequest.B

	return nil
}

func main() {
	sumService := &SumService{}
	rpc.Register(sumService)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":1234")

	if err != nil {
		log.Fatal("listen error:", err)
	}

	fmt.Println("Listening on port 1234")
	http.Serve(listener, nil)
}
