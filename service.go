package main

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ACL stores list of available rpc methods for each consumer
type RPCServer struct {
	UnimplementedAdminServer
	UnimplementedBizServer

	ACL map[string][]string

	eventChans []chan *Event
	mu         *sync.Mutex
}

var ErrUnauthenticated = status.Error(
	codes.Unauthenticated,
	"no cosumer metadata in rpc request or method not allowed",
)

func (s *RPCServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (s *RPCServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (s *RPCServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func NewRPCServer(ACL map[string][]string) *RPCServer {
	return &RPCServer{
		ACL:        ACL,
		eventChans: []chan *Event{},
		mu:         &sync.Mutex{},
	}
}

func StartMyMicroservice(ctx context.Context, addr string, acl string) error {
	ACL, err := getACL(acl)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		RPCServer := NewRPCServer(ACL)

		s := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				RPCServer.authUnaryInterceptor,
				RPCServer.logUnaryInterceptor,
			),
			grpc.ChainStreamInterceptor(
				RPCServer.authStreamInterceptor,
				RPCServer.logStreamInterceptor,
			),
		)

		RegisterAdminServer(s, RPCServer)
		RegisterBizServer(s, RPCServer)

		go func() {
			<-ctx.Done()
			s.GracefulStop()
		}()

		s.Serve(lis)
	}()

	return nil
}

func getACL(jsonACL string) (map[string][]string, error) {
	var ACL map[string][]string
	err := json.Unmarshal([]byte(jsonACL), &ACL)
	return ACL, err
}

func (s *RPCServer) authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if err := s.checkMethod(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *RPCServer) authStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if err := s.checkMethod(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *RPCServer) checkMethod(ctx context.Context, method string) error {
	md, _ := metadata.FromIncomingContext(ctx)

	values := md.Get("consumer")
	if len(values) == 0 {
		return ErrUnauthenticated
	}

	allowedMethods, ok := s.ACL[values[0]]
	if !ok {
		return ErrUnauthenticated
	}

	if slices.Contains(allowedMethods, method) {
		return nil
	}

	service := strings.Split(method, "/")[1]
	for _, m := range allowedMethods {
		serviceAndMethod := strings.Split(m, "/")
		if serviceAndMethod[1] == service && serviceAndMethod[2] == "*" {
			return nil
		}
	}

	return ErrUnauthenticated
}

func (s *RPCServer) logUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	s.sendLog(ctx, info.FullMethod)
	return handler(ctx, req)
}

func (s *RPCServer) logStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	s.sendLog(ss.Context(), info.FullMethod)
	return handler(srv, ss)
}

func (s *RPCServer) sendLog(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	p, _ := peer.FromContext(ctx)

	event := &Event{
		Timestamp: time.Now().Unix(),
		Consumer:  md.Get("consumer")[0],
		Method:    method,
		Host:      p.Addr.String(),
	}

	s.mu.Lock()
	for _, ch := range s.eventChans {
		ch <- event
	}
	s.mu.Unlock()
}

func (s *RPCServer) Logging(_ *Nothing, stream Admin_LoggingServer) error {
	ch := s.addEventListener()
	for {
		select {
		case event := <-ch:
			err := stream.Send(event)
			if err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *RPCServer) addEventListener() <-chan *Event {
	ch := make(chan *Event, 100)

	s.mu.Lock()
	s.eventChans = append(s.eventChans, ch)
	s.mu.Unlock()

	return ch
}

func (s *RPCServer) Statistics(interval *StatInterval, stream Admin_StatisticsServer) error {

	methodStat, consumerStat := map[string]uint64{}, map[string]uint64{}

	events := s.addEventListener()
	ticker := time.NewTicker(time.Duration(interval.IntervalSeconds) * time.Second)

	for {
		select {
		case event := <-events:
			methodStat[event.Method]++
			consumerStat[event.Consumer]++
		case t := <-ticker.C:
			err := stream.Send(&Stat{
				Timestamp:  t.Unix(),
				ByMethod:   methodStat,
				ByConsumer: consumerStat,
			})
			if err != nil {
				return err
			}

			methodStat, consumerStat = map[string]uint64{}, map[string]uint64{}
		case <-stream.Context().Done():
			return nil
		}
	}
}
