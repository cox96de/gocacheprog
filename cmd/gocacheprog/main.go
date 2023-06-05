package main

import (
	"context"
	"github.com/cox96de/gocacheprog/cache/disk"
	"log"
	"os"
	"path/filepath"

	"github.com/cox96de/gocacheprog/protocol"
)

func main() {
	cacheDir, err := os.UserCacheDir()
	checkError(err)
	create, err := os.Create("/tmp/log.log")
	checkError(err)
	defer create.Close()
	log.SetOutput(create)
	log.Printf("Using cache dir %s", cacheDir)
	err = protocol.Run(context.Background(), os.Stdin, os.Stdout,
		disk.NewDiskCache(filepath.Join(cacheDir, "gocacheprog")))
	checkError(err)
}

func checkError(err error) {
	if err == nil {
		return
	}
	panic(err)
}
