package grpc

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"io"
	"net"
	logger "github.com/Behxrad/log_streamer/api/proto"
	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/model"
	"github.com/Behxrad/log_streamer/internal/service"
	"github.com/Behxrad/log_streamer/pkg/lib"
	"time"
)

type Server interface {
	logger.LogServiceServer
	lib.Closable
	Serve() error
	Shutdown()
}

type rpcServer struct {
	logger.UnimplementedLogServiceServer
	gs *grpc.Server
	lg service.LogAggregator
	ws *service.WatchService
}

func NewRPCServer(aggregator service.LogAggregator, watchService *service.WatchService) Server {
	return &rpcServer{
		lg: aggregator,
		ws: watchService,
	}
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

func (s *rpcServer) Close(ctx context.Context) error {
	s.Shutdown()
	return nil
}

func (s *rpcServer) Shutdown() {
	s.gs.GracefulStop()
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
		for _, logEntry := range req.Logs {
			s.lg.Process(&model.LogEntry{
				ServiceName: logEntry.ServiceName,
				Level:       logEntry.Level,
				Message:     logEntry.Message,
				Timestamp:   time.UnixMilli(logEntry.Timestamp),
				MetaData:    logEntry.Metadata,
			})
		}
	}
}

func (s *rpcServer) WatchLogs(request *logger.WatchRequest, stream grpc.ServerStreamingServer[logger.LogEntry]) error {
	if request.ServiceName == "" {
		return status.Errorf(codes.InvalidArgument, "service_name is required")
	}

	channel := s.ws.Subscribe(request.ServiceName)

	for {
		select {
		case l, ok := <-channel:
			if !ok {
				return nil
			}
			log := &logger.LogEntry{
				ServiceName: l.ServiceName,
				Level:       l.Level,
				Message:     l.Message,
				Timestamp:   l.Timestamp.UnixMilli(),
				Metadata:    l.MetaData,
			}
			if err := stream.Send(log); err != nil {
				s.ws.Unsubscribe(request.ServiceName, channel)
				return err
			}
		case <-stream.Context().Done():
			s.ws.Unsubscribe(request.ServiceName, channel)
			return stream.Context().Err()
		}
	}
}
