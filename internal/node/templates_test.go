package node

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

type fakeTemplates struct{ err error }

func (f fakeTemplates) Register(context.Context, template.RegisterRequest) (template.Template, bool, error) {
	return template.Template{Name: "python"}, true, f.err
}

func (f fakeTemplates) Get(context.Context, string) (template.Template, error) {
	return template.Template{Name: "python"}, f.err
}

func (f fakeTemplates) List(context.Context, string, int) (template.Page, error) {
	return template.Page{Items: []template.Template{}}, f.err
}

func TestTemplateHTTPValidationAndStatusMapping(t *testing.T) {
	for _, test := range []struct {
		name   string
		body   string
		err    error
		status int
	}{
		{"created", `{"name":"python","snapshot":"golden"}`, nil, 201},
		{"unknown_field", `{"name":"python","snapshot":"golden","extra":true}`, nil, 400},
		{"multiple_objects", `{} {}`, nil, 400},
		{"oversized", strings.Repeat(" ", 64<<10) + `{}`, nil, 413},
		{"invalid_name", `{}`, template.ErrInvalid, 400},
		{"conflict", `{}`, template.ErrConflict, 409},
		{"missing", `{}`, &runtimeclient.RemoteError{Code: "NOT_FOUND"}, 404},
		{"unavailable", `{}`, &runtimeclient.RequestError{Cause: errors.New("private socket path")}, 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t, func(context.Context) error { return nil })
			server.templates = fakeTemplates{err: test.err}
			response := httptest.NewRecorder()
			server.handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/templates", strings.NewReader(test.body)))
			if response.Code != test.status {
				t.Fatalf("HTTP %d, want %d: %s", response.Code, test.status, response.Body.String())
			}
			if response.Code == 201 && response.Header().Get("Location") != "/v1/templates/python" {
				t.Fatal("missing template location")
			}
			if strings.Contains(response.Body.String(), "private socket") {
				t.Fatal("internal diagnostic leaked into API response")
			}
		})
	}
}

func TestTemplateReadRoutes(t *testing.T) {
	server := newServer(t, func(context.Context) error { return nil })
	for path, status := range map[string]int{
		"/v1/templates": 200, "/v1/templates/python": 200,
		"/v1/templates?limit=invalid": 400, "/v1/templates?after=%zz": 400,
	} {
		response := httptest.NewRecorder()
		server.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != status {
			t.Errorf("%s: HTTP %d, want %d", path, response.Code, status)
		}
	}
}
