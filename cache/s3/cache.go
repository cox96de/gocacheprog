package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/cox96de/gocacheprog/cache/disk"

	"github.com/cox96de/gocacheprog/protocol"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Cache is a cache implementation that stores data in a S3 bucket.
type Cache struct {
	s3     *s3.S3
	prefix string
	bucket string

	// Used to store data locally.
	// disk is the local disk cache for s3 server.
	disk *disk.DiskCache

	// Used to calculate cache hit rate.
	total atomic.Int32
	hit   atomic.Int32
	wg    sync.WaitGroup
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
	// Prefix is the base dir for S3.
	Prefix string
	// LocalDir is the local cache dir.
	LocalDir string
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
		prefix: opt.Prefix,
		disk:   disk.NewDiskCache(opt.LocalDir),
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

func (c *Cache) handleClose(_ context.Context, _ *protocol.ProgRequest, _ *protocol.ProgResponse) error {
	c.wg.Wait()
	hit := c.hit.Load()
	total := c.total.Load()
	log.Printf("s3 cache report, cache hit rate: %.2f(%d/%d)", float64(hit*100)/float64(total), hit, total)
	return nil
}

type meta struct {
	OutputID []byte `json:"o"`
	Size     int64  `json:"s"`
	Time     int64  `json:"t"`
}

func (c *Cache) handlePut(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
	if err := c.disk.HandlePut(ctx, req, resp); err != nil {
		return errors.WithStack(err)
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.putInS3(ctx, req); err != nil {
			// TODO: handle error properly
			log.Printf("failed to save to s3: %+v", err)
		}
	}()
	return nil
}

func (c *Cache) putInS3(ctx context.Context, req *protocol.ProgRequest) error {
	name := c.objectPath(req.ObjectID)
	if req.BodySize > 0 {
		_, err := req.Body.Seek(0, io.SeekStart)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = c.putFile(ctx, name, req.Body)
		if err != nil {
			return errors.WithStack(err)
		}
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
	return nil
}

func isS3NoSuchKeyError(err error) bool {
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		return true
	}
	return false
}

func (c *Cache) handleGet(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) (err error) {
	defer func() {
		c.total.Add(1)
		if !resp.Miss {
			c.hit.Add(1)
		}
	}()
	diskCacheError := c.disk.HandleGet(ctx, req, resp)
	if diskCacheError == nil && !resp.Miss {
		return nil
	}
	resp.Miss = false
	defer func() {
		if err != nil {
			resp.Miss = true
		}
	}()
	reader, err := c.getFile(ctx, c.actionPath(req.ActionID))
	if err != nil {
		if isS3NoSuchKeyError(err) {
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
	objectPath := c.objectPath(m.OutputID)
	object, err := c.getFile(ctx, objectPath)
	if err != nil {
		if isS3NoSuchKeyError(err) {
			resp.Miss = true
			return nil
		}
		return errors.WithStack(err)
	}
	all, err = io.ReadAll(object.Body)
	if err != nil {
		return errors.WithStack(err)
	}
	req.Body = bytes.NewReader(all)
	req.BodySize = m.Size
	req.ObjectID = m.OutputID
	err = c.disk.HandlePut(ctx, req, resp)
	if err != nil {
		return errors.WithStack(err)
	}
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
		Key:    aws.String(filepath.Join(c.prefix, path)),
		Bucket: &c.bucket,
	})
}

func (c *Cache) putFile(ctx context.Context, path string, body io.ReadSeeker) (*s3.PutObjectOutput, error) {
	return c.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    aws.String(filepath.Join(c.prefix, path)),
		Body:   body,
		Bucket: &c.bucket,
	})
}
