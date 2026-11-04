package search

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFlattenImportedJSONNumbers(t *testing.T) {
	fields := map[string][]string{}
	addFlattenedFields(fields, []string{"normal"}, map[string]any{"large": json.Number("9007199254740993"), "decimal": json.Number("0.123456789012345678901")})
	if !reflect.DeepEqual(fields["large"], []string{"9.007199254740993e+15"}) || !reflect.DeepEqual(fields["decimal"], []string{"0.123456789012345678901"}) {
		t.Fatalf("numeric fields lost or rounded: %v", fields)
	}
}

func TestReviewNumericTokensStable(t *testing.T) {
	for _, tc := range []struct {
		number string
		value  float64
		want   string
	}{
		{"1000000.00", 1e6, "1e+06"}, {"1e2", 100, "100"}, {"0.00001", 1e-5, "1e-05"}, {"0.0001", 1e-4, "0.0001"}, {"1.25e8", 125000000, "1.25e+08"}, {"-0.0", 0, "0"},
	} {
		for _, value := range []any{json.Number(tc.number), tc.value} {
			fields := map[string][]string{}
			addFlattenedFields(fields, []string{"normal", "n"}, value)
			if !reflect.DeepEqual(fields["n"], []string{tc.want}) {
				t.Errorf("%T %v: got %v want %s", value, value, fields, tc.want)
			}
		}
	}
}
