package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCommandHelpVersionAndInvalidFlags(t *testing.T) {
	for _, test := range []struct {
		args   []string
		code   int
		output string
	}{
		{[]string{"--help"}, 0, "Usage: kumaboxd"},
		{[]string{"--version"}, 0, "kumaboxd "},
		{[]string{"--unknown"}, 2, ""},
	} {
		var output, diagnostics bytes.Buffer
		code := run(context.Background(), test.args, func(string) string { return "" }, &output, &diagnostics)
		if code != test.code || !strings.Contains(output.String(), test.output) {
			t.Errorf("%v: exit %d output %q error %q", test.args, code, output.String(), diagnostics.String())
		}
		if code == 0 && diagnostics.Len() != 0 {
			t.Errorf("unexpected diagnostic: %s", diagnostics.String())
		}
	}
}
