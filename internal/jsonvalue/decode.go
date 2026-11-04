// Package jsonvalue decodes arbitrary JSON attributes without rounding numbers.
package jsonvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Unmarshal preserves interface-valued numbers while retaining the normal
// decoding rules for concrete numeric fields. Exactly one JSON value is required.
func Unmarshal(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return nil
}
