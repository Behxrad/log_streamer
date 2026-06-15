package grpc

import (
	"context"
	"io"
	logger "github.com/Behxrad/log_streamer/api/proto"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const testAddress = "localhost:50051"

type LogCase struct {
	Service string
	Level   string
	Message string
	Meta    map[string]string
}

func dial(t *testing.T) *grpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		testAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	return conn
}

func TestIngestAndWatchLogs_E2E(t *testing.T) {
	conn := dial(t)
	defer conn.Close()

	client := logger.NewLogServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Start WATCH FIRST
	watchStream, err := client.WatchLogs(ctx, &logger.WatchRequest{
		ServiceName: "test-service",
	})
	if err != nil {
		t.Fatalf("watch failed: %v", err)
	}

	// channel for received logs
	received := make(chan *logger.LogEntry, 10)
	errCh := make(chan error, 1)

	go func() {
		for {
			msg, err := watchStream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- err
				return
			}
			received <- msg
		}
	}()

	// 2. Define test cases
	cases := []LogCase{
		{
			Service: "test-service",
			Level:   "INFO",
			Message: "first log",
			Meta:    map[string]string{"env": "test"},
		},
		{
			Service: "test-service",
			Level:   "ERROR",
			Message: "something broke",
			Meta:    map[string]string{"error_code": "500"},
		},
	}

	// 3. Send logs via IngestLogs
	stream, err := client.IngestLogs(ctx)
	if err != nil {
		t.Fatalf("ingest stream failed: %v", err)
	}

	for _, c := range cases {
		err := stream.Send(&logger.LogChunk{
			Logs: []*logger.LogEntry{
				{
					ServiceName: c.Service,
					Level:       c.Level,
					Message:     c.Message,
					Timestamp:   time.Now().Unix(),
					Metadata:    c.Meta,
				},
			},
		})
		if err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("ingest not successful: %s", resp.Message)
	}

	// 4. Validate Watch output
	timeout := time.After(5 * time.Second)

	for i := 0; i < len(cases); i++ {
		select {
		case log := <-received:
			expected := cases[i]

			if log.ServiceName != expected.Service {
				t.Fatalf("service mismatch: got %s want %s", log.ServiceName, expected.Service)
			}
			if log.Level != expected.Level {
				t.Fatalf("level mismatch: got %s want %s", log.Level, expected.Level)
			}
			if log.Message != expected.Message {
				t.Fatalf("message mismatch: got %s want %s", log.Message, expected.Message)
			}

		case err := <-errCh:
			t.Fatalf("watch error: %v", err)

		case <-timeout:
			t.Fatal("timeout waiting for logs")
		}
	}
}
