Migrate objects from HCP object store to MinIO

# Usage

```
NAME:
  hcp-to-minio list - List objects in HCP namespace and download to disk

USAGE:
  hcp-to-minio list --auth-token --namespace-url --host-header --dir

FLAGS:
  --auth-token value, -a value     authorization token for HCP
  --namespace-url value, -n value  namespace URL path, e.g https://namespace-name.tenant-name.hcp-domain-name/rest
  --host-header value              host header for HCP
  --data-dir value, -d value       path to work directory for tool
  --annotation value               custom annotation name
  --insecure, -i                   disable TLS certificate verification
  --log, -l                        enable logging
  --debug                          enable debugging
  --help, -h                       show help
  

EXAMPLES:
1. List objects in HCP namespace https://hcp-vip.example.com and download list to /tmp/data
     $ hcp-to-minio list --a "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" --host-header "HOST:s3testbucket.tenant.hcp.example.com" \
                  --namespace-url "https://hcp-vip.example.com" --dir "/tmp/data"
```

## Example

> migrate objects from HCP data store to bucket "s3testbucket" of namespace `https://finance.europe.hcp.example.com/rest` to MinIO object store at endpoint https://minio:9000 using the output of `hcp-to-minio list` command

```
$ export MINIO_ACCESS_KEY=minio
$ export MINIO_SECRET_KEY=minio123
$ export MINIO_ENDPOINT=https://minio:9000
$ export MINIO_BUCKET=newbucket  # optional, if unspecified HCP bucket name is used

$ mkdir /tmp/data # temporary dir where output of listing is stored.
$ hcp-to-minio migrate --namespace-url https://finance.europe.hcp.example.com \
   --auth-token "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" \
   --host-header "s3testbucket.sandbox.hcp.example.com" \
   --data-dir /mnt/data \
   --bucket s3testbucket \
   --annotation myannotation \
   --input-file /tmp/data/to-migrate.txt
```
