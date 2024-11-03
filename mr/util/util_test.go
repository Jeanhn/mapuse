package util

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestItoa(t *testing.T) {
	i := int64(100)
	s := I64ToString(i)
	if s != "100" {
		t.Error()
	}
}

func TestSplit(t *testing.T) {
	s := "1 1"
	arr := strings.Split(s, " ")
	if len(arr) != 2 {
		t.Fatal(fmt.Sprint(arr))
	}

	b := []byte(s)
	bytesArr := bytes.Split(b, []byte(" "))
	if len(bytesArr) != 2 {
		t.Fatal(fmt.Sprint(bytesArr))
	}

	arr, err := UnmarshalKeyAndValue(b)
	if err != nil {
		t.Error(err)
	}
	if len(bytesArr) != 2 {
		t.Fatal(fmt.Sprint(arr))
	}

	bt := byte(' ')
	if int(bt) != 32 {
		t.Fail()
	}
}
