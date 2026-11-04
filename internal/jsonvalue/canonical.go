package jsonvalue

import (
	"encoding/json"
	"math/big"
	"strings"
)

// CanonicalNumber compares JSON numbers by their exact decimal value, including
// PostgreSQL JSONB's equivalent exponent/decimal spellings. It never expands an
// exponent into a large allocation or converts through a floating-point value.
func CanonicalNumber(number json.Number) json.Number {
	raw := string(number)
	sign := ""
	if strings.HasPrefix(raw, "-") {
		sign = "-"
		raw = raw[1:]
	}
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(raw), "e")
	scale := new(big.Int)
	if hasExponent {
		if _, ok := scale.SetString(exponent, 10); !ok {
			return number
		}
	}
	whole, fraction, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return json.Number("0")
	}
	scale.Sub(scale, big.NewInt(int64(len(fraction))))
	trimmed := strings.TrimRight(digits, "0")
	scale.Add(scale, big.NewInt(int64(len(digits)-len(trimmed))))
	if scale.Sign() == 0 {
		return json.Number(sign + trimmed)
	}
	return json.Number(sign + trimmed + "e" + scale.String())
}

// CanonicalBytes keeps keys and array order intact while normalizing numeric
// spellings for deterministic state digests and idempotent migration sync.
func CanonicalBytes(data []byte) ([]byte, error) {
	var value any
	if err := Unmarshal(data, &value); err != nil {
		return nil, err
	}
	var normalize func(any) any
	normalize = func(value any) any {
		switch typed := value.(type) {
		case json.Number:
			return CanonicalNumber(typed)
		case map[string]any:
			for key, child := range typed {
				typed[key] = normalize(child)
			}
		case []any:
			for i, child := range typed {
				typed[i] = normalize(child)
			}
		}
		return value
	}
	return json.Marshal(normalize(value))
}
