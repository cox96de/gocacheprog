# gocacheprog

## Usage

### Disk backend

```shell
export GOCACHEPROG="/app/gocacheprog --cache disk --disk-path=/tmp/gocache"
go build -v ./...
````

### S3 backend

```shell
export GOCACHEPROG="/app/gocacheprog --cache s3 --s3-access-key=${YOUR_S3_ACCESS_KEY} --s3-secret-key=${YOUR_S3_SECRET_KEY} --s3-endpoint=${YOUR_S3_ENDPOING} --s3-bucket=${YOUR_S3_BUCKET}"
go build -v ./...
```