module github.com/sgoel2be24-cyber/conveyor-job-queue

go 1.27.0

require (
	connectrpc.com/connect v1.20.0
	github.com/spf13/cobra v1.10.2
	golang.org/x/sys v0.47.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
)

tool (
	connectrpc.com/connect/cmd/protoc-gen-connect-go
	google.golang.org/protobuf/cmd/protoc-gen-go
)
