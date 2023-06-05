package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cox96de/gocacheprog/protocol"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type meta struct {
	OutputID []byte `json:"o"`
	Size     int64  `json:"s"`
	Time     int64  `json:"t"`
}

type DiskCache struct {
	dir string
}

func NewDiskCache(dir string) *DiskCache {
	return &DiskCache{dir: dir}
}

func (c *DiskCache) KnownCommands() ([]protocol.ProgCmd, error) {
	return []protocol.ProgCmd{protocol.CmdGet, protocol.CmdPut, protocol.CmdClose}, nil
}

func (c *DiskCache) Handler(ctx context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
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

func (c *DiskCache) handleClose(context.Context, *protocol.ProgRequest, *protocol.ProgResponse) error {
	return nil
}

func (c *DiskCache) actionPath(id []byte) string {
	return c.fileName(id, "a")
}

func (c *DiskCache) handleGet(_ context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) (err error) {
	defer func() {
		if err != nil {
			resp.Miss = true
		}
	}()
	f, err := os.Open(c.actionPath(req.ActionID))
	if err != nil {
		if os.IsNotExist(err) {
			resp.Miss = true
			return nil
		}
		return errors.WithStack(err)
	}
	all, err := io.ReadAll(f)
	if err != nil {
		return errors.WithStack(err)
	}
	m := &meta{}
	err = json.Unmarshal(all, m)
	if err != nil {
		return errors.WithStack(err)
	}
	resp.DiskPath = c.objectPath(m.OutputID)
	resp.Size = m.Size
	unix := time.Unix(m.Time, 0)
	resp.Time = &unix
	return nil
}

func (c *DiskCache) objectPath(id []byte) string {
	return c.fileName(id, "o")
}

func (c *DiskCache) handlePut(_ context.Context, req *protocol.ProgRequest, resp *protocol.ProgResponse) error {
	name := c.objectPath(req.ObjectID)
	err := os.MkdirAll(filepath.Dir(name), 0755)
	if err != nil {
		return err
	}
	f, err := os.Create(name)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	if req.BodySize > 0 {
		_, err = io.Copy(f, req.Body)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	m := &meta{
		OutputID: req.ObjectID,
		Size:     req.BodySize,
		Time:     time.Now().Unix(),
	}
	all, err := json.Marshal(m)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.MkdirAll(filepath.Dir(c.fileName(req.ActionID, "a")), 0755)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.WriteFile(c.fileName(req.ActionID, "a"), all, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	resp.DiskPath = name
	return nil
}

// fileName returns the name of the file corresponding to the given id.
func (c *DiskCache) fileName(id []byte, key string) string {
	return filepath.Join(c.dir, fmt.Sprintf("%02x", id[0]), fmt.Sprintf("%x", id)+"-"+key)
}
