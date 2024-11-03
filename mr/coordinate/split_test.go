package coordinate

import (
	"os"
	"testing"

	"mapuce/mr/data"
	"mapuce/mr/util"
)

func randomTaskId() string {
	id, _ := data.Default().IdGenerate()
	return "test" + util.I64ToString(id)
}

func TestMerge(t *testing.T) {
	_, err := merge([]string{"demo.txt", "demo2.txt"}, randomTaskId())
	if err != nil {
		t.Error(err)
	}
}

func TestPrivateIterate(t *testing.T) {
	se, err := NewSplitExecutor([]string{"demo.txt", "demo2.txt"}, 50, randomTaskId())
	if err != nil {
		t.Error(err)
	}

	f, err := os.OpenFile("test-merge.txt", os.O_CREATE|os.O_RDWR, 0666)
	defer f.Close()
	if err != nil {
		t.Error(err)
	}

	contents, err := se.iterate()
	if err != nil {
		t.Error(err)
	}
	for len(contents) != 0 {
		for _, line := range contents {
			_, err = f.WriteString(line)
			if err != nil {
				t.Error(err)
			}
		}
		f.WriteString("\r\n")
		contents, err = se.iterate()
		if err != nil {
			t.Error(err)
		}
	}
}

func TestIterate(t *testing.T) {
	se, _ := NewSplitExecutor([]string{"demo.txt", "demo2.txt"}, 50, randomTaskId())
	next, err := se.Iterate()
	if err != nil {
		t.Fatal(err)
	}
	for next {
		next, err = se.Iterate()
		if err != nil {
			t.Fatal(err)
		}
	}
}
