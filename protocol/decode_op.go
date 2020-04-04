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
	opDecoder = map[int32]func(RPCHeader, io.Reader) (Request, error){
		2001: decodeUpdateOp,
		2002: decodeInsertOp,
		2004: decodeQueryOp,
		2005: decodeGetMoreOp,
		2006: decodeDeleteOp,
		2007: decodeKillCursorsOp,
		2013: decodeMsgOp, // mongo 3.6+
	}

	// Register decoders for mongo commands wrapped in query ops. If the
	// decoder encounters an unknown command, it will fallback to emitting
	// a CommandRequest.
	//
	// See https://docs.mongodb.com/manual/reference/command
	cmdDecoder = map[string]func(RPCHeader, NamespacedCollection, bson.M, ReplyType) (Request, error){
		"insert":        decodeInsertCommand,
		"update":        decodeUpdateCommand,
		"delete":        decodeDeleteCommand,
		"find":          decodeFindCommand,
		"findAndModify": decodeFindAndModifyCommand,
	}
)

// Decode a request sent in by a mongo client.
func Decode(req []byte) (Request, error) {
	r := bytes.NewReader(req)

	hdr, err := decodeHeader(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode request header: %w", err)
	}

	dec := opDecoder[hdr.Opcode]
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
func decodeHeader(r io.Reader) (RPCHeader, error) {
	var hdr RPCHeader

	if err := binary.Read(r, binary.LittleEndian, &hdr.MessageLength); err != nil {
		return RPCHeader{}, xerrors.Errorf("unable to read message length field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.RequestID); err != nil {
		return RPCHeader{}, xerrors.Errorf("unable to read request ID field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.ResponseTo); err != nil {
		return RPCHeader{}, xerrors.Errorf("unable to read response to field: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &hdr.Opcode); err != nil {
		return RPCHeader{}, xerrors.Errorf("unable to read opcode field: %w", err)
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
func decodeUpdateOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for update op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.PayloadLength()-4)
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
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeUpdate},
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
func decodeInsertOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Parse flags
	var flags InsertFlag
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for insert op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.PayloadLength()-4)
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
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeInsert},
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
func decodeGetMoreOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for getMore op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.PayloadLength()-4)
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
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeGetMore, ReplyType: ReplyTypeOpReply},

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
func decodeDeleteOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Skip reserved int32 item
	var reserved int32
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return nil, xerrors.Errorf("unable to reserved reserved field for delete op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.PayloadLength()-4)
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
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeDelete},

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
func decodeKillCursorsOp(hdr RPCHeader, r io.Reader) (Request, error) {
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
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeKillCursors},

		CursorIDs: cursorIDs,
	}, nil
}

// decodeQueryOp unpacks an query operation message using the following
// schema:
//
//   struct OP_QUERY {
//       int32     flags;                  // bit vector of query options.  See below for details.
//       cstring   fullCollectionName ;    // "dbname.collectionname"
//       int32     numberToSkip;           // number of documents to skip
//       int32     numberToReturn;         // number of documents to return
//                                         //  in the first OP_REPLY batch
//       document  query;                  // query object.  See below for details.
//     [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
//                                         //  to return.  See below for details.
//   }
//
// Notes:
// - Mongod always sends a reply to query operations.
// - The query document may instead contain a mongo command. In case of a
//   command such as insert/update/delete, the decoder will coerce the request
//   into the inteded request type and force its replyExpected field to true.
func decodeQueryOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Parse flags
	var flags QueryFlag
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for query op: %w", err)
	}

	// Parse namespace
	nsCol, err := decodeNamespacedCollection(r, hdr.PayloadLength()-4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read namespaced collection for query op: %w", err)
	}

	// Parse skip/limit
	var numToSkip, numToReturn int32
	if err := binary.Read(r, binary.LittleEndian, &numToSkip); err != nil {
		return nil, xerrors.Errorf("unable to read number of docs to skip for query op: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &numToReturn); err != nil {
		return nil, xerrors.Errorf("unable to read number of docs to return for query op: %w", err)
	}

	// Read query doc
	queryDoc, err := decodeBSONDocument(r)
	if err != nil {
		return nil, xerrors.Errorf("unable to read query doc for query op: %w", err)
	}

	// Read optional field selector doc
	fieldSelectorDoc, err := decodeBSONDocument(r)
	if err != nil {
		if !xerrors.Is(err, io.EOF) {
			return nil, xerrors.Errorf("unable to read field selector doc for query op: %w", err)
		}
		fieldSelectorDoc = bson.D{}
	}

	// If this is not a command return back a QueryRequest.
	if nsCol.Collection != "$cmd" {
		return &QueryRequest{
			RequestInfo:   RequestInfo{Header: hdr, RequestType: RequestTypeQuery, ReplyType: ReplyTypeOpReply},
			Collection:    nsCol,
			Flags:         flags,
			NumToSkip:     numToSkip,
			NumToReturn:   numToReturn,
			Query:         queryDoc.Map(),
			FieldSelector: fieldSelectorDoc.Map(),
		}, nil
	}

	if len(queryDoc) == 0 {
		return nil, xerrors.Errorf("malformed query command")
	}

	// Lookup target collection for command and override the decoded $cmd value.
	cmdName := queryDoc[0].Name
	if colName, isString := queryDoc[0].Value.(string); isString {
		nsCol.Collection = colName
	}

	// Convert the query doc into a map and strip out the command name field.
	cmdArgs := queryDoc.Map()
	delete(cmdArgs, cmdName)

	// Locate a suitable decoder for the command and use OP_REPLY for
	// responses since this is an OP_QUERY request.
	if dec := cmdDecoder[cmdName]; dec != nil {
		return dec(hdr, nsCol, cmdArgs, ReplyTypeOpReply)
	}

	// Fallback to wrapping this as a generic command
	return &CommandRequest{
		// This request requires a reply to be sent back to the client
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeCommand, ReplyType: ReplyTypeOpReply},
		Collection:  nsCol,
		Command:     cmdName,
		Args:        cmdArgs,
	}, nil
}

// decodeMsgOp unpacks a generic message operation request. According to the
// docs (https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-msg)
// the following schema is used:
//
//   OP_MSG {
//       uint32 flagBits;           // message flags
//       Sections[] sections;       // data sections
//       optional<uint32> checksum; // optional CRC-32C checksum
//   }
//
//
// Each section starts with a byte that indicates its kind:
// - kind 0: A body section is encoded as a single BSON object.
// - kind 1: Document sequence with the following schema:
//     {
//        int32 size;  /// Size of the section in bytes.
//        cstring seq; // Document sequence identifier. In all current commands this field is the (possibly nested) field that it is replacing from the body section.
//        document*;   // Zero or more BSON objects
//     }
func decodeMsgOp(hdr RPCHeader, r io.Reader) (Request, error) {
	// Parse flags
	var flags uint32
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, xerrors.Errorf("unable to read flags for msg op: %w", err)
	}

	// Read sections until we run out of data. According to the detailed
	// OP_MSG spec (https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#specification)
	// the message must contain one bodySection and 0 or more docSeqSections
	type docSeqSection struct {
		// A (possibly nested) path in opMsgBodySection to override
		path string
		// The doc list to inject into the body section. This must be
		// a []interface{} to make this compatible with command parsers.
		docList []interface{}
	}

	var (
		bodySection    bson.D
		docSeqSections []docSeqSection
	)
	for section := 0; ; section++ {
		var kind uint8
		if err := binary.Read(r, binary.LittleEndian, &kind); err != nil {
			if err == io.EOF {
				break // finished reading sections
			}
			return nil, xerrors.Errorf("unable to read kind for section at index %d in msg op: %w", section, err)
		}

		switch kind {
		case 0: // command encoded as BSON object
			cmdDoc, err := decodeBSONDocument(r)
			if err != nil {
				return nil, xerrors.Errorf("unable to read body for section of type %d at index %d in msg op: %w", kind, section, err)
			}

			if len(cmdDoc) == 0 {
				return nil, xerrors.Errorf("malformed command in section of type %d at index %d in msg op", kind, section)
			}

			bodySection = cmdDoc
		case 1:
			// Parse size
			var size uint32
			if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
				return nil, xerrors.Errorf("unable to read size for section of type %d at index %d in msg op: %w", kind, section, err)
			}

			// Ensure we don't read more than size bytes for the section contents.
			sectionReader := io.LimitReader(r, int64(size)-4)

			path, err := decodeCString(sectionReader, int(size))
			if err != nil {
				return nil, xerrors.Errorf("unable to parse override path in section of type %d at index %d in msg op: %w", kind, section, err)
			}

			// Read docs; commands parsers expect doc lists to be []interface{}
			var docs []interface{}
			for docIdx := 0; ; docIdx++ {
				doc, err := decodeBSONDocument(sectionReader)
				if err != nil {
					if xerrors.Is(err, io.EOF) {
						break
					}

					return nil, xerrors.Errorf("unable to parse doc at index %d in section of type %d at index %d in msg op: %w", docIdx, kind, section, err)
				}
				docs = append(docs, doc)
			}

			docSeqSections = append(docSeqSections, docSeqSection{
				path:    path,
				docList: docs,
			})
		default:
			return nil, xerrors.Errorf("unknown type %d for section at index %d in msg op", kind, section)
		}
	}

	// If bit 0 of the flags is set, a crc32 is also included in the request
	if flags&0x1 == 0x1 {
		var crc32 uint32
		if err := binary.Read(r, binary.LittleEndian, &crc32); err != nil {
			return nil, xerrors.Errorf("unable to read CRC32 value for msg op: %w", err)
		}
	}

	// Sanity checks
	if len(bodySection) == 0 {
		return nil, xerrors.Errorf("unable to parse msg op: no type 0 section present")
	} else if len(docSeqSections) > 1 {
		return nil, xerrors.Errorf("unable to parse msg op: parser only supports up to one section of type 1")
	}

	// Extract collection and command names from body section
	var nsCol NamespacedCollection
	cmdName := bodySection[0].Name
	if colName, isString := bodySection[0].Value.(string); isString {
		nsCol.Collection = colName
	}

	cmdArgs := bodySection.Map()
	delete(cmdArgs, cmdName)
	if dbName, valid := cmdArgs["$db"].(string); valid && dbName != "" {
		nsCol.Database = dbName
		delete(cmdArgs, "$db")
	}

	// If a document list is provided as a type 1 payload, inject it to the
	// requested path.
	if len(docSeqSections) == 1 {
		sec := docSeqSections[0]
		// This is probably fine; pulling out args for type 0 payloads
		// is only supported for non-nested paths anyway.
		if strings.ContainsRune(sec.path, '.') {
			return nil, xerrors.Errorf("unable to parse msg op: parser does not support nested paths for type 1 payloads")
		}
		cmdArgs[sec.path] = sec.docList
	}

	// Locate a suitable decoder for the command
	if dec := cmdDecoder[cmdName]; dec != nil {
		// Since the incoming request uses OP_MSG as its envelope,
		// make sure that decoded requests signal that an OP_MSG reply
		// is required
		req, err := dec(hdr, nsCol, cmdArgs, ReplyTypeOpMsg)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse command %q in msg op: %w", cmdName, err)
		}
		return req, err
	}

	// Fallback to wrapping this as a generic command which expects a reply
	// using an OP_MSG response.
	return &CommandRequest{
		// This request requires a reply to be sent back to the client
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeCommand, ReplyType: ReplyTypeOpMsg},
		Collection:  nsCol,
		Command:     cmdName,
		Args:        cmdArgs,
	}, nil
}

// decodeUnknownOp is invoked when the reader encounters an unknown opcode.
func decodeUnknownOp(hdr RPCHeader, r io.Reader) (Request, error) {
	payload, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return &UnknownRequest{
		RequestInfo: RequestInfo{Header: hdr, RequestType: RequestTypeUnknown},
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
