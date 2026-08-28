package idempotency_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/forgeflow/forgeflow/internal/application/idempotency"
)

func TestHashPayload(t *testing.T) {
	t.Run("deterministic hashing", func(t *testing.T) {
		payload := []byte(`{"task":"test","param":123}`)
		hash1 := idempotency.HashPayload(payload)
		hash2 := idempotency.HashPayload(payload)

		assert.NotEmpty(t, hash1)
		assert.Equal(t, hash1, hash2)
	})

	t.Run("different payloads produce different hashes", func(t *testing.T) {
		hashA := idempotency.HashPayload([]byte(`{"task":"A"}`))
		hashB := idempotency.HashPayload([]byte(`{"task":"B"}`))

		assert.NotEqual(t, hashA, hashB)
	})

	t.Run("empty payload hash", func(t *testing.T) {
		hashEmpty := idempotency.HashPayload([]byte{})
		assert.NotEmpty(t, hashEmpty)
		assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hashEmpty)
	})
}
