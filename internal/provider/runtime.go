package provider

import (
	"context"
	"github.com/yourorg/sandboxing/pkg/models"
	"io"
)

type Terminal interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close()
	Resize(uint16, uint16)
}
type Runtime interface {
	Provider
	ExecStream(context.Context, string, string, chan<- models.ExecFrame) error
	ExecWithStdin(context.Context, string, string, io.Reader) error
	OpenInteractive(context.Context, string, []string) (io.WriteCloser, io.ReadCloser, error)
	OpenPTY(context.Context, string) (Terminal, error)
	GetMetrics(context.Context, string) (*models.ResourceMetrics, error)
}
type Snapshotter interface {
	CreateVolumeSnapshot(context.Context, string, string) (int64, error)
	RestoreVolume(context.Context, string, string, int) error
	DeleteVolumeSnapshot(context.Context, string) error
}
