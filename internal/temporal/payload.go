// Decoding Temporal payloads (inputs, results, memos, search attributes,
// heartbeat details) into JSON values.
//
// A payload is bytes plus an "encoding" metadata entry. json/plain payloads
// become their JSON value, binary/null becomes null, and anything else
// (protobuf, encrypted or codec-encoded payloads) is kept as
// {"encoding": ..., "data_base64": ...} so nothing is silently lost.

package temporal

import (
	"encoding/base64"
	"encoding/json"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// decodePayload returns a JSON-marshalable value for one payload.
func decodePayload(p *commonpb.Payload) any {
	if p == nil {
		return nil
	}
	enc := string(p.GetMetadata()["encoding"])
	switch enc {
	case "json/plain":
		var v any
		if json.Unmarshal(p.GetData(), &v) == nil {
			return v
		}
	case "binary/null":
		return nil
	case "json/protobuf":
		var v any
		if json.Unmarshal(p.GetData(), &v) == nil {
			return v
		}
	}
	out := map[string]any{"encoding": enc, "data_base64": base64.StdEncoding.EncodeToString(p.GetData())}
	if mt := p.GetMetadata()["messageType"]; len(mt) > 0 {
		out["message_type"] = string(mt)
	}
	return out
}

// decodePayloads decodes a Payloads list (nil when absent).
func decodePayloads(p *commonpb.Payloads) any {
	if p == nil || len(p.GetPayloads()) == 0 {
		return nil
	}
	out := make([]any, len(p.GetPayloads()))
	for i, x := range p.GetPayloads() {
		out[i] = decodePayload(x)
	}
	return out
}

// payloadMap decodes a map of payloads (memo fields, search attributes).
func payloadMap(m map[string]*commonpb.Payload) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, p := range m {
		out[k] = decodePayload(p)
	}
	return out
}

// jsonOrNil marshals a map to a JSON column value, nil for empty.
func jsonOrNil(m map[string]any) any {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return jsonText(b)
}

var protoJSON = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}

// eventAttributesJSON renders an event's attributes (the populated oneof) as
// JSON, with payloads decoded in place.
func eventAttributesJSON(ev *historypb.HistoryEvent) any {
	var msg proto.Message
	m := ev.ProtoReflect()
	od := m.Descriptor().Oneofs().ByName("attributes")
	if od == nil {
		return nil
	}
	fd := m.WhichOneof(od)
	if fd == nil {
		return nil
	}
	msg = m.Get(fd).Message().Interface()
	return protoToJSON(msg)
}

// protoToJSON renders a message as JSON with payloads decoded.
func protoToJSON(msg proto.Message) any {
	if msg == nil {
		return nil
	}
	raw, err := protoJSON.Marshal(msg)
	if err != nil {
		return nil
	}
	var tree any
	if json.Unmarshal(raw, &tree) != nil {
		return jsonText(raw)
	}
	b, err := json.Marshal(decodeTree(tree))
	if err != nil {
		return jsonText(raw)
	}
	return jsonText(b)
}

// decodeTree replaces every protojson-rendered Payload object
// ({"metadata": {"encoding": <base64>}, "data": <base64>}) with its value,
// and every Payloads wrapper with the list of values.
func decodeTree(v any) any {
	switch x := v.(type) {
	case map[string]any:
		if p, ok := asPayload(x); ok {
			return decodePayload(p)
		}
		// A Payloads message ({"payloads": [...]}) becomes the plain list.
		if list, ok := x["payloads"].([]any); ok && len(x) == 1 {
			return decodeTree(list)
		}
		for k, c := range x {
			x[k] = decodeTree(c)
		}
		return x
	case []any:
		for i, c := range x {
			x[i] = decodeTree(c)
		}
		return x
	}
	return v
}

func asPayload(m map[string]any) (*commonpb.Payload, bool) {
	md, ok := m["metadata"].(map[string]any)
	if !ok || len(m) > 3 {
		return nil, false
	}
	for k := range m {
		if k != "metadata" && k != "data" && k != "externalPayloads" {
			return nil, false
		}
	}
	if _, ok := md["encoding"].(string); !ok {
		return nil, false
	}
	p := &commonpb.Payload{Metadata: map[string][]byte{}}
	for k, val := range md {
		s, ok := val.(string)
		if !ok {
			return nil, false
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, false
		}
		p.Metadata[k] = b
	}
	if d, ok := m["data"].(string); ok {
		b, err := base64.StdEncoding.DecodeString(d)
		if err != nil {
			return nil, false
		}
		p.Data = b
	}
	return p, true
}
