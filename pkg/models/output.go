package models

import (
	"bytes"
	"fmt"
)

type OutputBuffer struct{ bytes.Buffer }

func (b *OutputBuffer) Write(data []byte) (int, error) {
	const limit = 2 << 20
	if b.Len()+len(data) > limit {
		return 0, fmt.Errorf("execution output exceeds 2 MiB; use streaming")
	}
	return b.Buffer.Write(data)
}
