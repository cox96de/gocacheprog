package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"io"
	"log"
	"path/filepath"
	"time"

	"github.com/cox96de/gocacheprog/protocol"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Cache is a cache implementation that stores data in a S3 bucket.
type Cache struct {
	s3      *s3.S3
	baseDir string
	bucket  string
}

// Option is the option for S3 cache.
type Option struct {
	// AccessKey is the access key for S3.
	AccessKey string
	// SecretKey is the secret key for S3.
	SecretKey string
	// Endpoint is the endpoint for S3.
	Endpoint string
	// Bucket is the bucket for S3.
	Bucket string
	// Region is the region for S3.
	Region string
}

func NewCache(opt *Option) (*Cache, error) {
	conf := aws.Config{
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
		Credentials: credentials.NewChainCredentials([]credentials.Provider{&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     opt.AccessKey,
				SecretAccessKey: opt.SecretKey,
			},
		}}),
		Region:   aws.String(opt.Region),
		Endpoint: &opt.Endpoint,
	}
	s, err := session.NewSessionWithOptions(session.Options{Config: conf})
	if err != nil {
		return nil, err
	}
	s3cli := s3.New(s)
	return &Cache{
		s3:     s3cli,
		bucket: opt.Bucket,
	}, nil
}

func (c *Cache) Handler(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
	switch req.Command {
	case protocol.CmdClose:
		return c.handleClose(ctx, req, resp)
	case protocol.CmdGet:
		err := c.handleGet(ctx, req, resp)
		if err != nil {
			log.Printf("failed to handle get: %v", err)
		}
		return err
	case protocol.CmdPut:
		err := c.handlePut(ctx, req, resp)
		if err != nil {
			log.Printf("failed to handle put: %v", err)
		}
		return err
	}
	return nil
}

func (c *Cache) KnownCommands() ([]protocol.ProgCmd, error) {
	return []protocol.ProgCmd{protocol.CmdGet, protocol.CmdPut, protocol.CmdClose}, nil
}

func (c *Cache) handleClose(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
	return nil
}

type meta struct {
	OutputID []byte `json:"o"`
	Size     int64  `json:"s"`
	Time     int64  `json:"t"`
}

func (c *Cache) handlePut(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
	name := c.objectPath(req.ObjectID)
	if req.BodySize > 0 {
		c.putFile(ctx, name, req.Body)
	}
	// TODO: meta might be not necessary, remove it, use s3's metadata instead
	m := &meta{
		OutputID: req.ObjectID,
		Size:     req.BodySize,
		Time:     time.Now().Unix(),
	}
	all, err := json.Marshal(m)
	actionFilepath := c.actionPath(req.ActionID)
	_, err = c.putFile(ctx, actionFilepath, aws.ReadSeekCloser(bytes.NewReader(all)))
	if err != nil {
		return err
	}
	resp.DiskPath = name
	return nil
}

func (c *Cache) handleGet(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) (err error) {
	defer func() {
		if err != nil {
			resp.Miss = true
		}
	}()
	reader, err := c.getFile(ctx, c.actionPath(req.ActionID))
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			resp.Miss = true
			return nil
		}
		return errors.WithStack(err)
	}
	all, err := io.ReadAll(reader.Body)
	if err != nil {
		return errors.WithStack(err)
	}
	m := &meta{}
	err = json.Unmarshal(all, m)
	if err != nil {
		return errors.WithStack(err)
	}
	resp.Size = m.Size
	unix := time.Unix(m.Time, 0)
	resp.Time = &unix
	return nil
}

func (c *Cache) objectPath(id []byte) string {
	return c.fileName(id, "o")
}

func (c *Cache) actionPath(id []byte) string {
	return c.fileName(id, "a")
}

// fileName returns the name of the file corresponding to the given id.
func (c *Cache) fileName(id []byte, key string) string {
	return filepath.Join(fmt.Sprintf("%02x", id[0]), fmt.Sprintf("%x", id)+"-"+key)
}

func (c *Cache) getFile(ctx context.Context, path string) (*s3.GetObjectOutput, error) {
	return c.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Key:    aws.String(filepath.Join(c.baseDir, path)),
		Bucket: &c.bucket,
	})
}

func (c *Cache) putFile(ctx context.Context, path string, body io.ReadSeeker) (*s3.PutObjectOutput, error) {
	// TODO: save a copy in local, as a cache for s3
	return c.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    aws.String(filepath.Join(c.baseDir, path)),
		Body:   body,
		Bucket: &c.bucket,
	})
}
