package main

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ACL stores list of available rpc methods for each consumer
type RPCServer struct {
	AdminLogServer
	BusinessServer

	ACL map[string][]string
	// мб хранить тут список тех кому надо отправлять логи
}

var ErrUnauthenticated = status.Error(
	codes.Unauthenticated,
	"no cosumer metadata in rpc request or method not allowed",
)

func NewRPCServer(ACL map[string][]string) *RPCServer {
	return &RPCServer{
		AdminLogServer: AdminLogServer{},
		BusinessServer: BusinessServer{},
		ACL:            ACL,
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
			grpc.UnaryInterceptor(RPCServer.authUnaryInterceptor),
			grpc.StreamInterceptor(RPCServer.authStreamInterceptor),
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
