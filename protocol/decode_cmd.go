package protocol

import (
	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

// decodeInsertCommand decodes an insert command packed within a query operation
// using the schema described in https://docs.mongodb.com/manual/reference/command/insert/#dbcmd.insert.
func decodeInsertCommand(hdr header, nsCol NamespacedCollection, cmdArgs bson.M) (Request, error) {
	docList, isDocList := cmdArgs["documents"].([]interface{})
	if !isDocList {
		return nil, xerrors.Errorf("malformed insert command in query doc: invalid doc list")
	}
	docs := make([]bson.M, len(docList))
	for i, d := range docList {
		doc, isDoc := d.(bson.D)
		if !isDoc {
			return nil, xerrors.Errorf("malformed insert command in query doc: invalid doc at index %d", i)
		}
		docs[i] = doc.Map()
	}

	req := &InsertRequest{
		// This request requires a reply to be sent back to the client
		requestBase: requestBase{h: hdr, reqType: RequestTypeInsert, replyExpected: true},
		Collection:  nsCol,
		Inserts:     docs,
	}

	if ordered, valid := cmdArgs["ordered"].(bool); valid && !ordered {
		req.Flags |= InsertFlagContinueOnError
	}

	return req, nil
}

// decodeUpdateCommand decodes an update command packed within a query operation
// using the schema described in https://docs.mongodb.com/manual/reference/command/update/#dbcmd.update
func decodeUpdateCommand(hdr header, nsCol NamespacedCollection, cmdArgs bson.M) (Request, error) {
	updatesDoc, valid := cmdArgs["updates"].([]interface{})
	if !valid {
		return nil, xerrors.Errorf("malformed update command in query doc: invalid updates list")
	}

	updateTargets := make([]UpdateTarget, len(updatesDoc))
	for i := 0; i < len(updatesDoc); i++ {
		updateDoc, valid := updatesDoc[i].(bson.D)
		if !valid {
			return nil, xerrors.Errorf("malformed update command in query doc: invalid update doc at index %d", i)
		}

		updateDocMap := updateDoc.Map()
		if q, valid := updateDocMap["q"].(bson.D); valid {
			updateTargets[i].Selector = q.Map()
		}
		if u, valid := updateDocMap["u"].(bson.D); valid {
			updateTargets[i].Update = u.Map()
		}
		if upsert, valid := updateDocMap["upsert"].(bool); valid && upsert {
			updateTargets[i].Flags |= UpdateFlagUpsert
		}
		if multi, valid := updateDocMap["multi"].(bool); valid && multi {
			updateTargets[i].Flags |= UpdateFlagMulti
		}
		if arrayFilterList, valid := cmdArgs["arrayFilters"].([]interface{}); valid {
			for j, fdoc := range arrayFilterList {
				arrayFilter, valid := fdoc.(bson.D)
				if !valid {
					return nil, xerrors.Errorf("malformed update command in query doc: invalid update doc at index %d: invalid array filter at index %d", i, j)
				}
				updateTargets[i].ArrayFilters = append(updateTargets[i].ArrayFilters, arrayFilter.Map())
			}
		}
	}

	return &UpdateRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeUpdate, replyExpected: true},
		Collection:  nsCol,
		Updates:     updateTargets,
	}, nil
}

// decodeDeleteCommand decodes a delete command packed within a query operation
// using the schema described in https://docs.mongodb.com/manual/reference/command/delete/#dbcmd.delete
func decodeDeleteCommand(hdr header, nsCol NamespacedCollection, cmdArgs bson.M) (Request, error) {
	deletesDoc, valid := cmdArgs["deletes"].([]interface{})
	if !valid {
		return nil, xerrors.Errorf("malformed delete command in query doc: invalid deletes list")
	}

	deleteTargets := make([]DeleteTarget, len(deletesDoc))
	for i := 0; i < len(deletesDoc); i++ {
		deleteDoc, valid := deletesDoc[i].(bson.D)
		if !valid {
			return nil, xerrors.Errorf("malformed delete command in query doc: invalid delete doc at index %d", i)
		}

		deleteDocMap := deleteDoc.Map()
		if q, valid := deleteDocMap["q"].(bson.D); valid {
			deleteTargets[i].Selector = q.Map()
		}
		if limit, valid := deleteDocMap["limit"].(int); valid {
			deleteTargets[i].Limit = limit
		}
	}

	req := &DeleteRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeDelete, replyExpected: true},
		Collection:  nsCol,
		Deletes:     deleteTargets,
	}

	return req, nil
}

// decodeFindCommand decodes a delete command packed within a query operation
// using the schema described in https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find
func decodeFindCommand(hdr header, nsCol NamespacedCollection, cmdArgs bson.M) (Request, error) {
	var numToSkip, numToReturn int32
	if skip, valid := cmdArgs["skip"].(int); valid {
		numToSkip = int32(skip)
	}
	if limit, valid := cmdArgs["limit"].(int); valid {
		numToReturn = int32(limit)
	}

	req := &QueryRequest{
		requestBase: requestBase{h: hdr, reqType: RequestTypeQuery, replyExpected: true},
		Collection:  nsCol,
		NumToSkip:   numToSkip,
		NumToReturn: numToReturn,
	}

	if filter, valid := cmdArgs["filter"].(bson.D); valid {
		req.Query = filter.Map()
	}
	if projection, valid := cmdArgs["projection"].(bson.D); valid {
		req.FieldSelector = projection.Map()
	}
	if sort, valid := cmdArgs["sort"].(bson.D); valid {
		req.Sort = sort.Map()
	}

	return req, nil
}
