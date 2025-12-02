package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type BusinessServer struct {
	UnimplementedBizServer
}

// ACL stores list of available rpc methods for each consumer
type RPCServer struct {
	AdminLogServer
	BusinessServer

	ACL map[string][]string
	// мб хранить тут список тех кому надо отправлять логи
}

var ErrUnauthorized = errors.New("unauthorized: no cosumer metadata in rpc request")

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
	md, _ := metadata.FromIncomingContext(ctx)

	values := md.Get("consumer")
	if len(values) == 0 {
		return nil, ErrUnauthorized
	}

	methods, ok := s.ACL[values[0]]
	if !ok || !methodIsAllowed(methods, info.FullMethod) {
		return nil, ErrUnauthorized
	}

	return handler(ctx, req)
}

func methodIsAllowed(allowedMethods []string, method string) bool {
	if slices.Contains(allowedMethods, method) {
		return true
	}

	service := strings.Split(method, "/")[0]
	for _, m := range allowedMethods {
		serviceAndMethod := strings.Split(m, "/")
		if serviceAndMethod[0] == service && serviceAndMethod[1] == "*" {
			return true
		}
	}

	return false
}
