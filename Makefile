PROTO_DIR  := proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')

.PHONY: proto proto-deps proto-clean grun

grun:
	go run ./cmd/engine

proto: proto-deps
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

proto-deps:
	@command -v protoc-gen-go >/dev/null || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@command -v protoc-gen-go-grpc >/dev/null || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

proto-clean:
	find $(PROTO_DIR) -name '*.pb.go' -delete
