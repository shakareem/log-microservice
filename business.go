package main

import "context"

type BusinessServer struct {
	UnimplementedBizServer
}

func (BusinessServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (BusinessServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (BusinessServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
