package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Load(t *testing.T) {
	os.Clearenv()

	var panicMsg any
	func() {
		defer func() {
			panicMsg = recover()
		}()
		Load()
	}()

	require.NotNil(t, panicMsg, "ожидалась паника, но её не было")
}

func Test_getEnv(t *testing.T) {
	t.Run("переменная существует", func(t *testing.T) {
		os.Setenv("TEST_KEY", "custom-value")
		defer os.Unsetenv("TEST_KEY")

		got := getEnv("TEST_KEY", "default")
		assert.Equal(t, "custom-value", got)
	})

	t.Run("переменная не существует → fallback", func(t *testing.T) {
		os.Unsetenv("TEST_KEY")

		got := getEnv("TEST_KEY", "fallback-value")
		assert.Equal(t, "fallback-value", got)
	})

	t.Run("переменная существует, но пустая", func(t *testing.T) {
		os.Setenv("TEST_KEY", "")
		defer os.Unsetenv("TEST_KEY")

		got := getEnv("TEST_KEY", "default")
		assert.Equal(t, "", got, "должна возвращаться именно пустая строка")
	})
}
