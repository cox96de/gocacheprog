package main

import (
	"context"
	"github.com/cox96de/gocacheprog/cache/disk"
	"github.com/cox96de/gocacheprog/protocol"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"os"
)

type Flags struct {
	Cache string
	Disk  struct {
		Dir string
	}
}

func parseFlags(args []string) (*Flags, error) {
	f := &Flags{}
	flagSet := pflag.NewFlagSet("gocacheprog", pflag.ContinueOnError)
	flagSet.StringVar(&f.Cache, "cache", "", "cache type, support disk")
	flagSet.StringVar(&f.Disk.Dir, "disk-dir", "", "disk cache dir")
	err := flagSet.Parse(args)
	if err != nil {
		return f, err
	}
	return f, nil
}

func main() {
	flags, err := parseFlags(os.Args)
	checkError(err)
	cache, err := composeCache(flags)
	checkError(err)
	err = protocol.Run(context.Background(), os.Stdin, os.Stdout,
		cache)
	checkError(err)
}

func composeCache(c *Flags) (protocol.Handler, error) {
	switch c.Cache {
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
