.PHONY: proto brick-server test

all: brick-server

proto:
	mkdir -p src/view
	protoc --rust_out=./src/view --grpc_out=./src/view --plugin=protoc-gen-grpc=`which grpc_rust_plugin` protocol/brick.proto

test:
	cargo test