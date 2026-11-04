package jsonvalue

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

func TestFormatNumberExactAndBounded(t *testing.T) {
	for in, want := range map[string]string{
		"9007199254740993": "9.007199254740993e+15", "9007199254740992": "9.007199254740992e+15",
		"0.123456789012345678901": "0.123456789012345678901", "-0e99": "0",
		"100e99999999999999999999": "1e+100000000000000000001", "1e-99999999999999999999": "1e-99999999999999999999",
	} {
		if got := FormatNumber(json.Number(in)); got != want {
			t.Errorf("%s => %s want %s", in, got, want)
		}
	}
	for _, value := range []float64{1e-5, 1e-4, 1e5, 1e6, 1.2345e8, -1.5, math.SmallestNonzeroFloat64, math.MaxFloat64} {
		want := strconv.FormatFloat(value, 'g', -1, 64)
		for _, spelling := range []string{want, strconv.FormatFloat(value, 'f', -1, 64), strconv.FormatFloat(value, 'e', -1, 64)} {
			if got := FormatNumber(json.Number(spelling)); got != want {
				t.Errorf("%s => %s want %s", spelling, got, want)
			}
		}
	}
}
