package protocol

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

// Encode a response for the specified request ID and write it to w.
func Encode(w io.Writer, r Response, reqID int32) error {
	var (
		buf bytes.Buffer
		hdr = header{
			responseTo: reqID,
			opcode:     1, // reply
		}
	)

	// Write header; note: we will patch the length at the end
	if err := writeHeaderTo(&buf, hdr); err != nil {
		return xerrors.Errorf("unable to serialize reply header: %w", err)
	}

	// Write reply message
	if err := writeResponseTo(&buf, r); err != nil {
		return xerrors.Errorf("unable to serialize reply body: %w", err)
	}

	// Grab response data, patch the message length and write to w.
	resData := buf.Bytes()
	binary.LittleEndian.PutUint32(resData[0:4], uint32(len(resData)))
	_, err := w.Write(resData)
	return err
}

func writeHeaderTo(w io.Writer, hdr header) error {
	if err := binary.Write(w, binary.LittleEndian, hdr.messageLength); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, hdr.requestID); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, hdr.responseTo); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, hdr.opcode); err != nil {
		return err
	}

	return nil
}

func writeResponseTo(w io.Writer, r Response) error {
	if err := binary.Write(w, binary.LittleEndian, r.Flags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, r.CursorID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, r.StartingFrom); err != nil {
		return err
	}

	var docCount = int32(len(r.Documents))
	if err := binary.Write(w, binary.LittleEndian, docCount); err != nil {
		return err
	}

	// Serialize document list
	for docIndex, doc := range r.Documents {
		docData, err := bson.Marshal(doc)
		if err != nil {
			return xerrors.Errorf("unable to marshal reply doc at index %d: %w", docIndex, w)
		}

		if _, err := w.Write(docData); err != nil {
			return xerrors.Errorf("unable to write marshaled reply doc at index %d: %w", docIndex, w)
		}
	}
	return nil
}
