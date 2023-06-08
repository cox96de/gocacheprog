package main

import (
	"gotest.tools/v3/assert"
	"testing"
)

func Test_parseFlags(t *testing.T) {
	t.Run("disk", func(t *testing.T) {
		flags, err := parseFlags([]string{"bin", "--cache=disk", "--disk-dir=/root/.cache/gocacheprog"})
		assert.NilError(t, err)
		assert.DeepEqual(t, flags, &Flags{
			Cache: "disk",
			Disk: struct {
				Dir string
			}{
				Dir: "/root/.cache/gocacheprog",
			},
			S3: struct {
				AccessKey string
				SecretKey string
				Endpoint  string
				Bucket    string
				Region    string
			}{
				Region: "default",
			},
		})
	})
	t.Run("s3", func(t *testing.T) {
		flags, err := parseFlags([]string{"bin", "--cache=s3", "--s3-access-key=ROOTNAME", "--s3-secret-key=CHANGEME123"})
		assert.NilError(t, err)
		assert.DeepEqual(t, flags, &Flags{
			Cache: "s3",
			Disk: struct {
				Dir string
			}{
				Dir: "/tmp/gocacheprog",
			},
			S3: struct {
				AccessKey string
				SecretKey string
				Endpoint  string
				Bucket    string
				Region    string
			}{
				AccessKey: "ROOTNAME",
				SecretKey: "CHANGEME123",
				Region:    "default",
			},
		})
	})
}
