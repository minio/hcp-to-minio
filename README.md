
Migrate objects from HCP object store to MinIO 

# Usage

```
USAGE:
  migratehcp [FLAGS]

FLAGS:
  --auth-token value, -a value     authorization token for HCP
  --namespace-url value, -n value  namespace URL path, e.g https://namespace-name.tenant-name.hcp-domain-name/rest
  --host-header value              host header for HCP
  --data-dir value, -d value       path to work directory for tool
  --bucket value                   bucket/name space directory
  --annotation value               custom annotation name
  --insecure, -i                   disable TLS certificate verification
  --log, -l                        enable logging
  --debug                          enable debugging
  --help, -h                       show help
  --version, -v                    print the version

```

## Example

> migrate objects in HCP data store in directory "quarterly_reports" of namespace `https://finance.europe.hcp.example.com/rest` to MinIO object store.

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_BUCKET=newbucket  # optional, if unspecified HCP bucket name is used

$ migratehcp  --n https://finance.europe.hcp.example.com/rest --a "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" --host-header "finance.europe.hcp.example.com" --data-dir /mnt/data --bucket quarterly_reports --annotation myannotation
```

