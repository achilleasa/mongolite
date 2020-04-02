package protocol

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

// Encode a response for the specified request ID and write it to w.
func Encode(w io.Writer, r Response, reqID int32, replyType ReplyType) error {
	var (
		buf      bytes.Buffer
		hdr      = header{responseTo: reqID}
		encodeFn func(io.Writer, Response) error
	)

	switch replyType {
	case ReplyTypeNone:
		return nil // nothing to do
	case ReplyTypeOpReply:
		hdr.opcode = 1 // OP_REPLY
		encodeFn = writeOpReplyTo
	case ReplyTypeOpMsg:
		hdr.opcode = 2013 // OP_MSG
		encodeFn = writeOpMsgTo
	}

	// Write header; note: we will patch the length at the end
	if err := writeHeaderTo(&buf, hdr); err != nil {
		return xerrors.Errorf("unable to serialize reply header: %w", err)
	}

	// Write reply message
	if err := encodeFn(&buf, r); err != nil {
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

// writeOpReplyTo encodes the response using the legacy OP_REPLY format. This
// is only used for OP_GETMORE and OP_QUERY requests.
func writeOpReplyTo(w io.Writer, r Response) error {
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

// writeOpMsgTo encodes the response using the OP_MSG format. This is used for
// encoding responses to OP_MSG requests that most modern mongo clients send in.
func writeOpMsgTo(w io.Writer, r Response) error {
	// Write OP_MSG flags
	var flags uint32
	if err := binary.Write(w, binary.LittleEndian, flags); err != nil {
		return err
	}

	// Accoding to the docs on mongo wire protocol, replies should use
	// a single section of type body (kind: 0) for encoding the response
	// payload.
	if len(r.Documents) > 1 {
		return xerrors.Errorf("OP_MSG payloads with multiple documents are not supported")
	}

	// Write section kind byte with a value of 0 to indicate a body section.
	var kind uint8
	if err := binary.Write(w, binary.LittleEndian, kind); err != nil {
		return err
	}

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
