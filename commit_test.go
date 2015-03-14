package main

import (
	"testing"
)

func TestNewlineCommitter(t *testing.T) {
	var committed string
	nlc := NewlineCommitter{CommitterFunc(func(buf []byte) int {
		committed = string(buf)
		return 0
	})}

	cases := []struct {
		buf, committed, remaining string
	}{
		{"first\nsecond", "first\n", "second"},
		{"first\n", "first\n", ""},
		{"\nsecond", "\n", "second"},
		{"noNewLine", "noNewLine", ""},
	}

	for _, c := range cases {
		buf := []byte(c.buf)
		remain := nlc.Commit(buf)
		after := string(buf[len(buf)-remain : len(buf)])

		t.Logf("buf=%q, commit=%q, after=%q", c.buf, committed, after)

		if after != c.remaining {
			t.Errorf("expected %q after, got %q", c.remaining, after)
		}

		if committed != c.committed {
			t.Errorf("expected %q committed, got %q", c.committed, committed)
		}
	}
}
