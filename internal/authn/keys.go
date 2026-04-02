package authn

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
)

type MemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string][]Key
}

func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{
		keys: make(map[string][]Key),
	}
}

func (s *MemoryKeyStore) Name() string {
	return "memory"
}

func (s *MemoryKeyStore) Put(key Key) error {
	if key.Principal.Name == "" {
		return fmt.Errorf("principal name is required")
	}
	if key.PublicKey == nil {
		return fmt.Errorf("public key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	index := lookupKey(key.Principal.Organization, key.Principal.Name)
	s.keys[index] = append(s.keys[index], key)
	return nil
}

func (s *MemoryKeyStore) Lookup(_ context.Context, userID, organization string) ([]Key, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Key
	if organization != "" {
		out = append(out, s.keys[lookupKey(organization, userID)]...)
	}
	out = append(out, s.keys[lookupKey("", userID)]...)

	copied := make([]Key, len(out))
	copy(copied, out)
	return copied, nil
}

func lookupKey(organization, name string) string {
	return organization + "\x00" + name
}

func ParseRSAPublicKeyPEM(data []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("decode PEM block: no PEM data found")
	}

	if key, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return key, nil
	}

	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	publicKey, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("parse public key: unexpected key type %T", parsed)
	}

	return publicKey, nil
}

func ParseRSAPrivateKeyPEM(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("decode PEM block: no PEM data found")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	privateKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("parse private key: unexpected key type %T", parsed)
	}

	return privateKey, nil
}
