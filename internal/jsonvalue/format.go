package jsonvalue

import (
	"encoding/json"
	"math/big"
	"strings"
)

// FormatNumber uses Go's shortest-float notation boundaries (exponent < -4
// or >= 6), but retains all decimal digits without converting to a float.
// Exponents remain bounded in memory even for numbers outside float64 range.
func FormatNumber(number json.Number) string {
	raw := string(CanonicalNumber(number))
	if raw == "0" {
		return raw
	}
	sign := ""
	if strings.HasPrefix(raw, "-") {
		sign = "-"
		raw = raw[1:]
	}
	digits, scaleText, _ := strings.Cut(raw, "e")
	scale := new(big.Int)
	if scaleText != "" {
		scale.SetString(scaleText, 10)
	}
	exponent := new(big.Int).Add(scale, big.NewInt(int64(len(digits)-1)))
	if exponent.Cmp(big.NewInt(-4)) < 0 || exponent.Cmp(big.NewInt(6)) >= 0 {
		mantissa := digits[:1]
		if len(digits) > 1 {
			mantissa += "." + digits[1:]
		}
		expSign := "+"
		if exponent.Sign() < 0 {
			expSign = "-"
		}
		exp := new(big.Int).Abs(exponent).String()
		if len(exp) < 2 {
			exp = "0" + exp
		}
		return sign + mantissa + "e" + expSign + exp
	}
	point := int(exponent.Int64()) + 1
	if point <= 0 {
		return sign + "0." + strings.Repeat("0", -point) + digits
	}
	if point >= len(digits) {
		return sign + digits + strings.Repeat("0", point-len(digits))
	}
	return sign + digits[:point] + "." + digits[point:]
}
