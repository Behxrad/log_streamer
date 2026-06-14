package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"log"
	"net"
	logger "github.com/Behxrad/log_streamer/api/proto"
	"github.com/Behxrad/log_streamer/configs"
)

type Server interface {
	logger.LogServiceServer
	Serve() error
	Shutdown() error
}

type rpcServer struct {
	logger.UnimplementedLogServiceServer
	gs *grpc.Server
}

func NewRPCServer() Server {
	return &rpcServer{}
}

func (s *rpcServer) Serve() error {
	lis, err := net.Listen("tcp", configs.Config.GRPCAddress)
	if err != nil {
		return err
	}
	s.gs = grpc.NewServer()
	logger.RegisterLogServiceServer(s.gs, s)
	reflection.Register(s.gs)
	if err := s.gs.Serve(lis); err != nil {
		return err
	}
	return nil
}

func (s *rpcServer) Shutdown() error {
	s.gs.GracefulStop()
	return nil
}

func (s *rpcServer) IngestLogs(stream grpc.ClientStreamingServer[logger.LogChunk, logger.StatusResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			err := stream.SendAndClose(&logger.StatusResponse{
				Success: true,
				Message: "parsed all",
			})
			if err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		log.Println(req.Logs)
	}
}

func (s *rpcServer) WatchLogs(request *logger.WatchRequest, stream grpc.ServerStreamingServer[logger.LogEntry]) error {
	return nil
}
