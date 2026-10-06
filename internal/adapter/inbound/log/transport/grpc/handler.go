package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	logger "github.com/Behxrad/log_streamer/api/proto"
	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/port/inbound/log"
	"github.com/Behxrad/log_streamer/pkg/lib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
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
	lg log.Aggregator
	ws log.Watcher
}

func NewRPCServer(aggregator log.Aggregator, watchService log.Watcher) Server {
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
			err := s.lg.Process(log.ProcessLogCommand{LogEntry: &log.LogEntry{
				ServiceName: logEntry.ServiceName,
				Level:       logEntry.Level,
				Message:     logEntry.Message,
				Timestamp:   time.UnixMilli(logEntry.Timestamp),
				Metadata:    logEntry.Metadata,
			}})
			if err != nil {
				return err
			}
		}
	}
}

func (s *rpcServer) WatchLogs(request *logger.WatchRequest, stream grpc.ServerStreamingServer[logger.LogEntry]) error {
	if request.ServiceName == "" {
		return status.Errorf(codes.InvalidArgument, "service_name is required")
	}

	channel, err := s.ws.Subscribe(log.SubscribeLogsQuery{ServiceName: request.ServiceName})
	if err != nil {
		return err
	}

	for {
		select {
		case l, ok := <-channel:
			if !ok {
				return nil
			}
			log_entry := &logger.LogEntry{
				ServiceName: l.ServiceName,
				Level:       l.Level,
				Message:     l.Message,
				Timestamp:   l.Timestamp.UnixMilli(),
				Metadata:    l.Metadata,
			}
			if err := stream.Send(log_entry); err != nil {
				inErr := s.ws.Unsubscribe(log.UnsubscribeLogsCommand{
					ServiceName: request.ServiceName,
					Stream:      channel,
				})
				if inErr != nil {
					return errors.Join(err, inErr)
				}
				return err
			}
		case <-stream.Context().Done():
			err := s.ws.Unsubscribe(log.UnsubscribeLogsCommand{
				ServiceName: request.ServiceName,
				Stream:      channel,
			})
			if err != nil {
				return errors.Join(stream.Context().Err(), err)
			}
			return stream.Context().Err()
		}
	}
}
