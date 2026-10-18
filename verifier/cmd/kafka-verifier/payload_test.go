package main

import "testing"

func TestPayloadRoundTripAndPlantedCorruption(t *testing.T) {
	payloadSize, corruptNth = 102400, 0
	for n, id := range []string{"cr-kafka-000000001", "cr-kafka-000000002"} {
		b := payload(id, n)
		if len(b) != 102400 {
			t.Fatalf("size %d", len(b))
		}
		got, ok := parsePayload(b)
		if got != id || !ok {
			t.Fatalf("round trip %q ok=%v", got, ok)
		}
		b[len(b)-1] ^= 1
		if _, ok := parsePayload(b); ok {
			t.Fatal("flipped last byte not detected")
		}
	}
	corruptNth = 3
	bad := 0
	for n := 0; n < 9; n++ {
		if _, ok := parsePayload(payload("cr-kafka-000000009", n)); !ok {
			bad++
		}
	}
	if bad != 2 { // n = 3, 6 (n = 0 is never corrupted)
		t.Fatalf("planted corruption count %d, want 2", bad)
	}
	payloadSize, corruptNth = 0, 0
	if id, ok := parsePayload([]byte("cr-kafka-000000003")); id != "cr-kafka-000000003" || !ok {
		t.Fatal("size-0 record must parse as-is")
	}
}
