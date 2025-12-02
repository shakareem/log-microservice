package main

type AdminLogServer struct {
	UnimplementedAdminServer
}

func (AdminLogServer) Logging(*Nothing, Admin_LoggingServer) error {
	// TODO
	return nil
}

func (AdminLogServer) Statistics(*StatInterval, Admin_StatisticsServer) error {
	// TODO
	return nil
}
