genproto:
	@find ./proto -name '*.proto' -exec protoc --proto_path=./proto --go_out=./gen --go_opt=paths=source_relative --go-vtproto_out=./gen --go-vtproto_opt=paths=source_relative {} +