package jsonvalue

import (
	"encoding/json"
	"testing"
)

func TestDecodePreservesNumbersAndTypedFields(t *testing.T) {
	var value struct {
		Count      int
		Attributes map[string]any
	}
	if err := Unmarshal([]byte(`{"Count":7,"Attributes":{"n":9007199254740993,"decimal":0.123456789012345678901}}`), &value); err != nil {
		t.Fatal(err)
	}
	if value.Count != 7 || value.Attributes["n"] != json.Number("9007199254740993") || value.Attributes["decimal"] != json.Number("0.123456789012345678901") {
		t.Fatalf("rounded values: %+v", value)
	}
	for _, raw := range []string{`{} {}`, `{} trailing`, `{"x":`} {
		if Unmarshal([]byte(raw), new(any)) == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
func TestCanonicalNumberExactDecimalEquivalence(t *testing.T) {
	for _, pair := range [][2]string{{"1e3", "1000"}, {"1.234567890123456789e40", "12345678901234567890000000000000000000000"}, {"-0.0001200", "-12e-5"}, {"-0.0", "0"}, {"123e+999999999999999999999", "123e999999999999999999999"}} {
		if CanonicalNumber(json.Number(pair[0])) != CanonicalNumber(json.Number(pair[1])) {
			t.Fatalf("equivalent numbers differ: %v", pair)
		}
	}
	if CanonicalNumber("9007199254740992") == CanonicalNumber("9007199254740993") {
		t.Fatal("adjacent integers collapsed")
	}
}
