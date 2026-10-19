package main

import "testing"

func TestValidatePprofListenAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{
			name:    "default IPv4 loopback",
			address: defaultPprofAddress,
		},
		{
			name:    "IPv6 loopback",
			address: "[::1]:6060",
		},
		{
			name:    "implicit wildcard",
			address: ":6060",
			wantErr: true,
		},
		{
			name:    "IPv4 wildcard",
			address: "0.0.0.0:6060",
			wantErr: true,
		},
		{
			name:    "non-loopback address",
			address: "192.0.2.10:6060",
			wantErr: true,
		},
		{
			name:    "hostname",
			address: "localhost:6060",
			wantErr: true,
		},
		{
			name:    "missing port",
			address: "127.0.0.1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validatePprofListenAddress(tt.address)
			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"validatePprofListenAddress(%q) error = %v, wantErr %v",
					tt.address,
					err,
					tt.wantErr,
				)
			}
		})
	}
}
