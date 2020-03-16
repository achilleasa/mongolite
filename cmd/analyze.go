package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/achilleasa/mongolite/protocol"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/xerrors"
	"gopkg.in/urfave/cli.v2"
)

// AnalyzeStream implements the analyze tool CLI command.
func AnalyzeStream(ctx *cli.Context) error {
	var reqStream io.Reader

	if ctx.NArg() != 1 {
		return xerrors.Errorf("No input file specified")
	}

	reqFile := ctx.Args().First()
	if reqFile == "-" {
		appLogger.WithField("from", "STDIN").Info("reading captured stream data")
		reqStream = os.Stdin
	} else {
		f, err := os.Open(reqFile)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		reqStream = f
		appLogger.WithField("from", reqFile).Info("reading captured stream data")
	}

	// Parse options
	var (
		offset    = ctx.Int("offset")
		limit     = ctx.Int("limit")
		filterMap map[protocol.RequestType]bool
	)
	if filterList := ctx.StringSlice("filter"); len(filterList) != 0 {
		knownReqTypes := make(map[string]struct{})
		for _, rType := range protocol.AllRequestTypeNames() {
			knownReqTypes[rType] = struct{}{}
		}

		filterMap = make(map[protocol.RequestType]bool)
		for _, filter := range filterList {
			if _, valid := knownReqTypes[filter]; !valid {
				return xerrors.Errorf("unknown request type %q in --filter parameter", filter)
			}
			filterMap[protocol.RequestType(filter)] = true
		}
	}

	return analyze(reqStream, offset, limit, filterMap)
}

func analyze(reqStream io.Reader, offset, limit int, filterMap map[protocol.RequestType]bool) error {
	// Apply requested offset
	for i := 0; i < offset; i++ {
		var rLen int32
		if err := binary.Read(reqStream, binary.LittleEndian, &rLen); err != nil {
			if err == io.EOF {
				break // tried to seek beyond EOF
			}
			return xerrors.Errorf("unable to read size of request %d: %w", i+1, err)
		}

		// Skip captured payload
		if _, err := io.CopyN(ioutil.Discard, reqStream, int64(rLen)); err != nil {
			return xerrors.Errorf("unable to skip over request %d: %w", i+1, err)
		}
	}

	// Run decode loop
	var (
		buf      bytes.Buffer
		indentRe = regexp.MustCompile("(?m)^")
	)
	for i := 0; ; i++ {
		if limit != 0 && i == limit {
			break
		}

		var rLen int32
		if err := binary.Read(reqStream, binary.LittleEndian, &rLen); err != nil {
			if err == io.EOF {
				break // tried to seek beyond EOF
			}
			return xerrors.Errorf("unable to read of request %d: %w", i+offset+1, err)
		}

		buf.Reset()
		if _, err := io.CopyN(&buf, reqStream, int64(rLen)); err != nil {
			return xerrors.Errorf("unable to read request %d: %w", i+offset+1, err)
		}

		req, err := protocol.Decode(buf.Bytes())
		if err != nil {
			return xerrors.Errorf("unable to decode request %d: %w", i+offset+1, err)
		}

		// Apply filtering
		if filterMap != nil && !filterMap[req.Type()] {
			continue
		}

		reqDump := indentRe.ReplaceAllString(spew.Sdump(req), "  ")
		fmt.Printf("[+] request: %05d, type %q (opcode: %d)\n%s\n", i, req.Type(), req.Opcode(), reqDump)
	}

	return nil
}
