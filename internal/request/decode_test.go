package request

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type decodeTestBody struct {
	io.Reader
	closed bool
}

func (b *decodeTestBody) Close() error {
	b.closed = true
	return nil
}

func TestDecodeJSONContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    string
		wantErr bool
		wantEOF bool
	}{
		{name: "array", body: `[{"id":1},{"id":2}]`},
		{name: "null", body: `null`},
		{name: "empty", wantErr: true, wantEOF: true},
		{name: "blank", body: " \n\r\t", wantErr: true, wantEOF: true},
		{name: "truncated", body: `[{"id":1}`, wantErr: true},
		{name: "wrong type", body: `{}`, wantErr: true},
		{name: "second value", body: `[] {}`, wantErr: true},
		{name: "second null", body: `[] null`, wantErr: true},
		{name: "trailing junk", body: `[] junk`, wantErr: true},
		{name: "trailing incomplete value", body: `[] "`, wantErr: true},
		{name: "trailing whitespace", body: "[] \n\t"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := &decodeTestBody{Reader: strings.NewReader(test.body)}
			resp := &http.Response{Body: body, ContentLength: 1 << 60}
			out := []struct{ ID int }{{ID: 99}}
			err := DecodeJSON(resp, &out)
			if (err != nil) != test.wantErr || errors.Is(err, io.EOF) != test.wantEOF {
				t.Fatalf("error = %v, want error=%v, EOF=%v", err, test.wantErr, test.wantEOF)
			}
			if test.wantEOF && (len(out) != 1 || out[0].ID != 99) {
				t.Fatal("empty body changed the decode target")
			}
			if body.closed {
				t.Fatal("decoder took body ownership from its caller")
			}
		})
	}
}

func TestDecodeJSONPreservesArrValues(t *testing.T) {
	var out struct {
		ID   int64  `json:"id"`
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	body := `{"id":9007199254740993,"path":"C:\\media\\A \u2603.mkv","size":3500000000,"ignored":{"nested":[1,2,3]}}`
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(body))}
	if err := DecodeJSON(resp, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 9007199254740993 || out.Size != 3500000000 || out.Path != "C:\\media\\A \u2603.mkv" {
		t.Fatalf("decoded values changed: %+v", out)
	}
}

func TestDecodeJSONNilInputs(t *testing.T) {
	var out any
	for _, resp := range []*http.Response{nil, {}} {
		if err := DecodeJSON(resp, &out); err != nil {
			t.Fatal(err)
		}
	}
	body := &decodeTestBody{Reader: strings.NewReader(`{}`)}
	if err := DecodeJSON(&http.Response{Body: body}, nil); err != nil || body.closed {
		t.Fatalf("nil target changed body ownership: %v", err)
	}
	if data, err := io.ReadAll(body); err != nil || string(data) != "{}" {
		t.Fatalf("nil target consumed response body: %q, %v", data, err)
	}
}

type decodeErrorReader struct{ err error }

func (r decodeErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestDecodeJSONPreservesReadErrors(t *testing.T) {
	want := errors.New("synthetic body read failure")
	for _, prefix := range []string{"", `{"id":`, `{"id":1}`} {
		t.Run(prefix, func(t *testing.T) {
			resp := &http.Response{Body: io.NopCloser(io.MultiReader(strings.NewReader(prefix), decodeErrorReader{want}))}
			var out any
			if err := DecodeJSON(resp, &out); !errors.Is(err, want) {
				t.Fatalf("decode lost original read error: %v", err)
			}
		})
	}
}

func TestDecodeJSONDoesNotMaterializeTrailingCollection(t *testing.T) {
	unwantedRead := errors.New("attempted to decode the second collection")
	for _, opening := range []string{"[", "{"} {
		resp := &http.Response{Body: io.NopCloser(io.MultiReader(
			strings.NewReader("{} "+opening), decodeErrorReader{unwantedRead},
		))}
		var out any
		if err := DecodeJSON(resp, &out); err == nil || errors.Is(err, unwantedRead) {
			t.Fatalf("trailing %s was not rejected at its opening token: %v", opening, err)
		}
	}
}
