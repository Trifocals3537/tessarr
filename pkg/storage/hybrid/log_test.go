package hybrid

import "testing"

func TestEncodedLogRecordSize(t *testing.T) {
	tests := []struct {
		name                                   string
		key, value, category, provider, status int
		entryName, protocol                    int
		wantSize                               int
		wantErr                                bool
	}{
		{
			name:      "normal record",
			key:       40,
			value:     1024,
			category:  8,
			provider:  12,
			status:    10,
			entryName: 120,
			protocol:  7,
			wantSize:  logRecordFixedBytes + 40 + 1024 + 8 + 12 + 10 + 120 + 7,
		},
		{
			name:     "oversized encoded metadata",
			category: maxUint16EncodedLength + 1,
			wantErr:  true,
		},
		{
			name:    "oversized key",
			key:     maxLogKeyBytes + 1,
			wantErr: true,
		},
		{
			name:    "oversized record",
			value:   maxLogRecordBytes,
			wantErr: true,
		},
		{
			name:    "negative length",
			key:     -1,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			size, err := encodedLogRecordSize(
				test.key,
				test.value,
				test.category,
				test.provider,
				test.status,
				test.entryName,
				test.protocol,
			)
			if (err != nil) != test.wantErr {
				t.Fatalf("encodedLogRecordSize() error = %v, wantErr %t", err, test.wantErr)
			}
			if !test.wantErr && (size < logRecordFixedBytes || size > maxLogRecordBytes) {
				t.Fatalf("encodedLogRecordSize() = %d outside allowed range", size)
			}
			if !test.wantErr && size != test.wantSize {
				t.Fatalf("encodedLogRecordSize() = %d, want %d", size, test.wantSize)
			}
		})
	}
}

func TestAppendLogReadRejectsInvalidSizes(t *testing.T) {
	log := &appendLog{}
	for _, size := range []int32{-1, maxLogRecordBytes + 1} {
		if _, err := log.ReadAt(0, size); err == nil {
			t.Fatalf("ReadAt(size=%d) error = nil", size)
		}
		if _, err := log.ReadAtInto(0, size, nil); err == nil {
			t.Fatalf("ReadAtInto(size=%d) error = nil", size)
		}
	}
}
