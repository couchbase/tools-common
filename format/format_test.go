package format

import (
	"testing"
	"time"
)

func TestBytes(t *testing.T) {
	inputs := []uint64{
		0,                                        // lower byte range
		500,                                      // mid byte range
		1023,                                     // high byte range
		1024,                                     // kb range
		1024 + 100,                               // mid kb range
		1024*1024 - 1024,                         // high kb range
		1024 * 1024,                              // low MB range
		1024*1024 + 1024*500,                     // mid MB range
		1024*1024*1024 - 1024*100,                // high MB range
		1024 * 1024 * 1024,                       // low gb range
		1024*1024*1024*2 + 1024*1024*100,         // mid gb range
		1024*1024*1024*1024 - 1024*1024*100,      // high gb range
		1024 * 1024 * 1024 * 1024,                // low Tb range
		1024*1024*1024*1024 + 1024*1024*1024*100, // mid tb range
		1024*1024*1024*1024*1024 - 1024*1024*1024*100,           // high TB range
		1024 * 1024 * 1024 * 1024 * 1024,                        // low PB range
		1024*1024*1024*1024*1024 + 1024*1024*1024*1024*500,      // mid PB range
		1024*1024*1024*1024*1024*1024 - 1024*1024*1024*1024*500, // high PB range
	}

	outputs := []string{
		"0B",
		"500B",
		"1023B",
		"1.00KiB",
		"1.10KiB",
		"1023.00KiB",
		"1.00MiB",
		"1.49MiB",
		"1023.90MiB",
		"1.00GiB",
		"2.10GiB",
		"1023.90GiB",
		"1.00TiB",
		"1.10TiB",
		"1023.90TiB",
		"1.00PiB",
		"1.49PiB",
		"1023.51PiB",
	}

	for ix := range outputs {
		if out := Bytes(inputs[ix]); out != outputs[ix] {
			t.Fatalf("Expected %s got %s", outputs[ix], out)
		}
	}
}

func TestDuration(t *testing.T) {
	type test struct {
		input    time.Duration
		expected string
	}

	tests := []*test{
		{
			input:    time.Second + 500*time.Millisecond,
			expected: "1.5s",
		},
		{
			input:    time.Minute + time.Second + 500*time.Millisecond,
			expected: "1m1s",
		},
	}

	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			actual := Duration(test.input)
			if actual != test.expected {
				t.Fatalf("Expected %s but got %s", test.expected, actual)
			}
		})
	}
}
