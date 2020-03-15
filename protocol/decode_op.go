package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"strings"

	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

var (
	// Register decoders for known mongo opcodes. If the decoder encounters
	// an unknown opcode, it will fallback to calling decodeUnknownOp.
	//
	// See https://docs.mongodb.com/manual/reference/mongodb-wire-protocol.
	opDecoder = map[int32]func(header, io.Reader) (Request, error){
		2001: decodeUpdateOp,
		2002: decodeInsertOp,
		2005: decodeGetMoreOp,
		2006: decodeDeleteOp,
		2007: decodeKillCursorsOp,
	}
)

// Decode a request sent in by a mongo client.
func Decode(req []byte) (Request, error) {
	r := bytes.NewReader(req)

	hdr, err := decodeHeader(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode request header: %w", err)
	}

	dec := opDecoder[hdr.opcode]
	if dec == nil {
		dec = decodeUnknownOp
	}

	// Pass the request body to the opcode decoder func
	decodedReq, err := dec(hdr, r)
	if err != nil {
		return nil, err
	}

	return decodedReq, nil
}

// decodeHeader reads a mongo request header from r.
func decodeHeader(r io.Reader) (header, error) {
	var hdr header

	if err := binary.Read(r, binary.LittleEndian, &hdr.messageLength); err != nil {
		return header{}, xerrors.Errorf("unable to read message length field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.requestID); err != nil {
		return header{}, xerrors.Errorf("unable to read request ID field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.responseTo); err != nil {
		return header{}, xerrors.Errorf("unable to read response to field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.opcode); err != nil {
		return header{}, xerrors.Errorf("unable to read opcode field: %w", err)
	}

	return hdr, nil
}

// decodeUpdateOp unpacks an update operation message using the following
// schema:
//
//   struct OP_UPDATE {
//       int32     ZERO;               // 0 - reserved for future use
//       cstring   fullCollectionName; // "dbname.collectionname"
//       int32     flags;              // bit vector. see below
//       document  selector;           // the query to select the document
//       document  update;             // specification of the update to perform
//   }
//
// Note: the server does not send a reply for update requests.
func decodeUpdateOp(hdr header, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for update op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.payloadLength()-4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read namespaced collection for update op: %w", err)
	}

	// Parse flags
	var flags UpdateFlag
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for update op: %w", err)
	}

	// Read selector doc
	selectorDoc, err := decodeBSONDocument(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to read selector doc for update op: %w", err)
	}

	// Read update doc
	updateDoc, err := decodeBSONDocument(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to read update doc for update op: %w", err)
	}

	return &UpdateRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeUpdate},
		Collection:  nsCol,
		Updates: []UpdateTarget{
			UpdateTarget{
				Selector: selectorDoc.Map(),
				Update:   updateDoc.Map(),
				Flags:    flags,
			},
		},
	}, nil
}

// decodeInsertOp unpacks an insert operation message using the following
// schema:
//
//   struct OP_INSERT {
//       int32     flags;              // bit vector - see below
//       cstring   fullCollectionName; // "dbname.collectionname"
//       document* documents;          // one or more documents to insert into the collection
//   }
//
// Note: the server does not send a reply for insert requests.
func decodeInsertOp(hdr header, r io.Reader) (Request, error) {
	// Parse flags
	var flags InsertFlag
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for insert op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.payloadLength()-4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read namespaced collection for insert op: %w", err)
	}

	// Read list of docs to insert until we consume the entire request.
	var docs []bson.M
	for {
		doc, err := decodeBSONDocument(r)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}
			return nil, xerrors.Errorf("unable to read doc list for insert op: %w", err)
		}
		docs = append(docs, doc.Map())
	}

	return &InsertRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeInsert},
		Collection:  nsCol,
		Flags:       flags,
		Inserts:     docs,
	}, nil
}

// decodeGetMoreOp unpacks a getMore operation message using the following
// schema:
//
//   struct {
//       int32     ZERO;               // 0 - reserved for future use
//       cstring   fullCollectionName; // "dbname.collectionname"
//       int32     numberToReturn;     // number of documents to return
//       int64     cursorID;           // cursorID from the OP_REPLY
//   }
//
// Note: the server always sends a reply for getMore requests.
func decodeGetMoreOp(hdr header, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for getMore op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.payloadLength()-4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read namespaced collection for getMore op: %w", err)
	}

	// Parse number of docs to return (batch size) and cursor ID
	var numToReturn int32
	if err := binary.Read(r, binary.LittleEndian, &numToReturn); err != nil {
		return nil, xerrors.Errorf("unable to read number to return for getMore op: %w", err)
	}

	var cursorID int64
	if err := binary.Read(r, binary.LittleEndian, &cursorID); err != nil {
		return nil, xerrors.Errorf("unable to read cursor ID for getMore op: %w", err)
	}

	return GetMoreRequest{
		// This request requires a reply to be sent back to the client
		requestBase: requestBase{h: hdr, reqType: RequestTypeGetMore, replyExpected: true},

		Collection:  nsCol,
		NumToReturn: numToReturn,
		CursorID:    cursorID,
	}, nil
}

// decodeDeleteOp unpacks a delete operation message using the following
// schema:
//
//   struct {
//       int32     ZERO;               // 0 - reserved for future use
//       cstring   fullCollectionName; // "dbname.collectionname"
//       int32     flags;              // bit vector
//       document  selector;           // query object
//   }
//
// Note: the server does not send a reply for delete requests.
func decodeDeleteOp(hdr header, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for delete op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.payloadLength()-4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read namespaced collection for delete op: %w", err)
	}

	// Parse flags.
	var flags int32
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for delete op: %w", err)
	}

	// If bit 0 of the delete flags is set to true, we should only delete
	// the first matching document, i.e. set limit = 1
	var limit int
	if flags&0x1 == 0x1 {
		limit = 1
	}

	// Read query doc
	queryDoc, err := decodeBSONDocument(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to read selector doc for delete op: %w", err)
	}

	return DeleteRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeDelete},

		Collection: nsCol,
		Deletes: []DeleteTarget{
			DeleteTarget{
				Selector: queryDoc.Map(),
				Limit:    limit,
			},
		},
	}, nil
}

// decodeKillCursorsOp unpacks a killCursors operation message using the
// following schema:
//
//   struct {
//       int32     ZERO;              // 0 - reserved for future use
//       int32     numberOfCursorIDs; // number of cursorIDs in message
//       int64*    cursorIDs;         // sequence of cursorIDs to close
//   }
//
// Note: the server does not sendsa reply for killCursors requests.
func decodeKillCursorsOp(hdr header, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for killCursors op: %w", err)
	}

	// Parse number cursor IDs that follow
	var numCursors int32
	if err := binary.Read(r, binary.LittleEndian, &numCursors); err != nil {
		return nil, xerrors.Errorf("unable to read number of cursor IDs for killCursors op: %w", err)
	}

	// Read cursor IDs
	var cursorIDs = make([]int64, numCursors)
	for i := 0; i < len(cursorIDs); i++ {
		if err := binary.Read(r, binary.LittleEndian, &cursorIDs[i]); err != nil {
			return nil, xerrors.Errorf("unable to read cursor ID at index %d for killCurosrs op: %w", i, err)
		}
	}

	return KillCursorsRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeKillCursors},

		CursorIDs: cursorIDs,
	}, nil
}

// decodeUnknownOp is invoked when the reader encounters an unknown opcode.
func decodeUnknownOp(hdr header, r io.Reader) (Request, error) {
	payload, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return &UnknownRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeUnknown},
		Payload:     payload,
	}, nil
}

// decodeNamespacedCollection reads a cstring from the stream and decodes it
// into a namespaced collection instance.
func decodeNamespacedCollection(r io.Reader, maxLen int) (NamespacedCollection, error) {
	cstring, err := decodeCString(r, maxLen)
	if err != nil {
		return NamespacedCollection{}, xerrors.Errorf("unable to decode namespaced collection: %w", err)
	}

	tokens := strings.SplitN(cstring, ".", 2)
	if len(tokens) != 2 {
		return NamespacedCollection{}, xerrors.Errorf("unable to decode namespaced collection: malformed namespace %q", cstring)
	} else if len(tokens[0]) == 0 {
		return NamespacedCollection{}, xerrors.Errorf("unable to decode namespaced collection: malformed namespace %q; empty database name", cstring)
	} else if len(tokens[1]) == 0 {
		return NamespacedCollection{}, xerrors.Errorf("unable to decode namespaced collection: malformed namespace %q; empty collection name", cstring)
	}

	return NamespacedCollection{
		Database:   tokens[0],
		Collection: tokens[1],
	}, nil
}

// decodeCString extracts a zero-terminated string from the stream or fails with
// an error if more than maxLen characters have been read.
func decodeCString(r io.Reader, maxLen int) (string, error) {
	var (
		buf       bytes.Buffer
		totalRead int
		nextByte  = make([]byte, 1)
	)
	for totalRead < maxLen {
		n, err := r.Read(nextByte)
		if err != nil {
			return "", xerrors.Errorf("unable to read cstring from stream: %w", err)
		} else if n != 1 {
			return "", xerrors.Errorf("unable to read cstring from stream: failed to read next byte", err)
		}
		if nextByte[0] == 0 { // found null terminator
			return buf.String(), nil
		}
		totalRead++
		if err = buf.WriteByte(nextByte[0]); err != nil {
			return "", xerrors.Errorf("unable to read cstring from stream: %w", err)
		}
	}

	return "", xerrors.Errorf("unable to read cstring from stream: exceeded max allowed string length without locating null terminator")
}

func decodeBSONDocument(r io.Reader) (bson.D, error) {
	var docSize int32
	if err := binary.Read(r, binary.LittleEndian, &docSize); err != nil {
		return nil, xerrors.Errorf("unable to read BSON doc from stream: %w", err)
	} else if docSize < 4 {
		return nil, xerrors.Errorf("unable to read BSON doc from stream: invalid doc size %d", docSize)
	}

	rawDocSize := int(docSize - 4)
	if rawDocSize == 0 {
		// This is just an empty document
		return bson.D{}, nil
	}

	// Allocate buffer for the doc and prepend the size back into it as a uint32
	doc := make([]byte, docSize)
	binary.LittleEndian.PutUint32(doc[0:4], uint32(docSize))

	// Now read the document data
	n, err := r.Read(doc[4:])
	if err != nil {
		return nil, xerrors.Errorf("unable to read BSON doc from stream: %w", err)
	} else if n != rawDocSize {
		return nil, xerrors.Errorf("read partial BSON doc from stream: expected to read %d bytes; got %d", rawDocSize, n)
	}

	// append buffers
	var bsonDoc bson.D
	if err = bson.Unmarshal(doc, &bsonDoc); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal BSON doc: %w", err)
	}
	return bsonDoc, nil
}
