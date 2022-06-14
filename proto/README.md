Create `storage.pb.go` and `storage_gorums.pb.go` with the following command:

```protoc -I=$(go list -m -f {{.Dir}} github.com/relab/gorums):. \
  --go_out=paths=source_relative:. \
  --gorums_out=paths=source_relative:. \
  storage.proto```