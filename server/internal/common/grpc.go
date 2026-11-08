package common

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// SignalContext returns a context cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ServeGRPC listens on port, registers the service, the standard gRPC health
// service (used by the Kubernetes grpc probes) and reflection, and serves
// until ctx is cancelled, then shuts down gracefully.
func ServeGRPC(ctx context.Context, name, port string, register func(*grpc.Server)) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("%s: listen on %s: %v", name, port, err)
	}

	srv := grpc.NewServer()
	register(srv)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		log.Printf("%s: shutting down", name)
		hs.Shutdown()
		done := make(chan struct{})
		go func() { srv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			srv.Stop()
		}
	}()

	log.Printf("%s listening on :%s", name, port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("%s: serve: %v", name, err)
	}
}

// RunEvery calls fn immediately and then every interval until ctx ends.
// A panic in fn is logged and does not stop the loop.
func RunEvery(ctx context.Context, name string, interval time.Duration, fn func(context.Context)) {
	run := func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("%s: panic: %v", name, r)
			}
		}()
		fn(ctx)
	}
	run()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}
