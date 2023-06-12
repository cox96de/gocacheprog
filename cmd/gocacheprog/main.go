package main

import (
	"context"
	"os"

	"github.com/cox96de/gocacheprog/cache/disk"
	"github.com/cox96de/gocacheprog/cache/s3"
	"github.com/cox96de/gocacheprog/protocol"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type Flags struct {
	Cache string
	Disk  struct {
		Dir string
	}
	S3 struct {
		AccessKey string
		SecretKey string
		Endpoint  string
		Bucket    string
		Region    string
		Prefix    string
	}
}

func parseFlags(args []string) (*Flags, error) {
	f := &Flags{}
	flagSet := pflag.NewFlagSet("gocacheprog", pflag.ContinueOnError)
	flagSet.StringVar(&f.Cache, "cache", "disk", "cache type, support disk and s3")
	flagSet.StringVar(&f.Disk.Dir, "disk-dir", "/tmp/gocacheprog", "disk cache dir")
	flagSet.StringVar(&f.S3.AccessKey, "s3-access-key", "", "s3 access key")
	flagSet.StringVar(&f.S3.SecretKey, "s3-secret-key", "", "s3 secret key")
	flagSet.StringVar(&f.S3.Endpoint, "s3-endpoint", "", "s3 endpoint")
	flagSet.StringVar(&f.S3.Bucket, "s3-bucket", "", "s3 bucket")
	flagSet.StringVar(&f.S3.Region, "s3-region", "default", "s3 region")
	flagSet.StringVar(&f.S3.Prefix, "s3-prefix", "gocacheprog", "s3 prefix")
	err := flagSet.Parse(args)
	if err != nil {
		flagSet.Usage()
		return f, err
	}
	return f, err
}

func main() {
	flags, err := parseFlags(os.Args)
	if err != nil {
		os.Exit(1)
	}
	cache, err := composeCache(flags)
	checkError(err)
	err = protocol.Run(context.Background(), os.Stdin, os.Stdout,
		cache)
	checkError(err)
}

func composeCache(c *Flags) (protocol.Handler, error) {
	switch c.Cache {
	case "s3":
		return s3.NewCache(&s3.Option{
			AccessKey: c.S3.AccessKey,
			SecretKey: c.S3.SecretKey,
			Endpoint:  c.S3.Endpoint,
			Bucket:    c.S3.Bucket,
			Region:    c.S3.Region,
			Prefix:    c.S3.Prefix,
			LocalDir:  c.Disk.Dir,
		})
	case "disk":
		return disk.NewDiskCache(c.Disk.Dir), nil
	default:
		return nil, errors.Errorf("unknown cache backend '%s'", c.Cache)
	}
}

func checkError(err error) {
	if err == nil {
		return
	}
	panic(err)
}
