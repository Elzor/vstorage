
test: prepare_grpc
	@pytest -s

prepare_grpc:
	@mkdir -p ./client
	@python -m grpc_tools.protoc -I../src/api/proto --python_out=./client --grpc_python_out=./client block_api.proto && touch ./client/__init__.py