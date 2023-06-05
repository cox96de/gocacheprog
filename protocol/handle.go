package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

type Handler interface {
	KnownCommands() ([]ProgCmd, error)
	Handler(ctx context.Context, req *ProgRequest, resp *ProgResponse) error
}

func Run(ctx context.Context, in io.Reader, out io.Writer, handler Handler) error {
	bufReader := bufio.NewReader(in)
	bufWriter := bufio.NewWriter(out)
	reqReader := json.NewDecoder(bufReader)
	respWriter := json.NewEncoder(bufWriter)
	cmds, err := handler.KnownCommands()
	if err != nil {
		return err
	}
	err = respWriter.Encode(&ProgResponse{
		KnownCommands: cmds,
	})
	if err != nil {
		return err
	}
	err = bufWriter.Flush()
	if err != nil {
		return err
	}
	lock := &sync.Mutex{}
	for {
		req := new(ProgRequest)
		err := reqReader.Decode(req)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if req.Command == CmdPut && req.BodySize > 0 {
			var body []byte
			if err = reqReader.Decode(&body); err != nil {
				return err
			}
			if len(body) != int(req.BodySize) {
				return errors.New("bad body size")
			}
			req.Body = bytes.NewReader(body)
		}
		go func() {
			resp := &ProgResponse{
				ID: req.ID,
			}
			if err = handler.Handler(ctx, req, resp); err != nil {
				resp.Err = err.Error()
			}
			lock.Lock()
			respWriter.Encode(resp)
			bufWriter.Flush()
			lock.Unlock()
		}()
	}
}
