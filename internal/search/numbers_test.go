package search

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFlattenImportedJSONNumbers(t *testing.T) {
	fields := map[string][]string{}
	addFlattenedFields(fields, []string{"normal"}, map[string]any{"large": json.Number("9007199254740993"), "decimal": json.Number("0.123456789012345678901")})
	if !reflect.DeepEqual(fields["large"], []string{"9007199254740993"}) || !reflect.DeepEqual(fields["decimal"], []string{"0.123456789012345678901"}) {
		t.Fatalf("numeric fields lost or rounded: %v", fields)
	}
}
