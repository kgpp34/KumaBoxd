package config

import (
	"io"
	"testing"
)

func TestFlagsOverrideEnvironment(t *testing.T) {
	environment := map[string]string{
		"KUMABOXD_LISTEN": "127.0.0.1:9000", "KUMABOXD_RUNTIME_SOCKET": "/tmp/runtime.sock",
	}
	cfg, err := Parse([]string{"--listen", "[::1]:8000"}, func(key string) string { return environment[key] }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "[::1]:8000" || cfg.RuntimeSocket != "/tmp/runtime.sock" {
		t.Fatalf("incorrect precedence: %+v", cfg)
	}
}

func TestRejectsInvalidStartup(t *testing.T) {
	for _, args := range [][]string{
		{"--listen", "0.0.0.0:8000"}, {"--listen", "localhost:8000"},
		{"--listen", "127.0.0.1:65536"}, {"--runtime-socket", "relative.sock"},
		{"--probe-timeout", "0s"}, {"--shutdown-timeout", "-1s"}, {"unexpected"},
	} {
		if _, err := Parse(args, func(string) string { return "" }, io.Discard); err == nil {
			t.Errorf("accepted invalid args %v", args)
		}
	}
}

func TestPoolDurabilityIsExplicitAndValidated(t *testing.T) {
	for _, mode := range []string{"sync", "buffered"} {
		cfg, err := Parse([]string{"--pool-durability=" + mode}, func(string) string { return "" }, io.Discard)
		if err != nil || cfg.PoolDurability != mode {
			t.Fatalf("mode %s: %+v %v", mode, cfg, err)
		}
	}
	if _, err := Parse([]string{"--pool-durability=unsafe"}, func(string) string { return "" }, io.Discard); err == nil {
		t.Fatal("unknown durability accepted")
	}
}
