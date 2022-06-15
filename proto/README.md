Files `storage.pb.go` and `storage_gorums.pb.go` are generated from `storage.proto` using Protocol buffers with the [Gorums](https://github.com/relab/gorums) plugin.
To regenerate run the following command:

```
protoc -I=$(go list -m -f {{.Dir}} github.com/relab/gorums):. \
  --go_out=paths=source_relative:. \
  --gorums_out=paths=source_relative:. \
  storage.proto
```

Check the Gorums repo for [installation instructions](https://github.com/relab/gorums/blob/master/doc/user-guide.md).