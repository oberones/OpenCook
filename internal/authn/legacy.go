package authn

import (
	"crypto/rsa"
	"fmt"
	"math/big"
)

func legacyPublicDecrypt(publicKey *rsa.PublicKey, ciphertext []byte) ([]byte, error) {
	k := publicKey.Size()
	if len(ciphertext) != k {
		return nil, fmt.Errorf("legacy signature length mismatch")
	}

	c := new(big.Int).SetBytes(ciphertext)
	if c.Cmp(publicKey.N) > 0 {
		return nil, fmt.Errorf("legacy signature out of range")
	}

	m := new(big.Int).Exp(c, big.NewInt(int64(publicKey.E)), publicKey.N)
	em := leftPad(m.Bytes(), k)

	return unpadLegacyPKCS1v15(em)
}

func unpadLegacyPKCS1v15(em []byte) ([]byte, error) {
	if len(em) < 11 {
		return nil, fmt.Errorf("legacy signature too short")
	}
	if em[0] != 0x00 || em[1] != 0x01 {
		return nil, fmt.Errorf("legacy signature has invalid padding prefix")
	}

	paddingEnd := -1
	for i := 2; i < len(em); i++ {
		if em[i] == 0x00 {
			paddingEnd = i
			break
		}
		if em[i] != 0xff {
			return nil, fmt.Errorf("legacy signature has invalid padding")
		}
	}

	if paddingEnd < 10 {
		return nil, fmt.Errorf("legacy signature padding too short")
	}

	return em[paddingEnd+1:], nil
}

func leftPad(in []byte, size int) []byte {
	if len(in) >= size {
		return in
	}

	out := make([]byte, size)
	copy(out[size-len(in):], in)
	return out
}
