package request

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// A pending marker is durable before a provider request starts. If the
// process exits before it sees a response, the next process cannot assume the
// request avoided a 429. An operator must resolve an uncertain outcome.
type gateDiskState struct {
	Status string `json:"status"`
	Until  string `json:"until,omitempty"`
}

const (
	gateIdle    = "idle"
	gatePending = "pending"
	gateBlocked = "blocked"
)

type gateJournal struct{ path string }

func openGateJournal(path string) (*gateJournal, gateDiskState, error) {
	dir := filepath.Dir(path)
	_, dirErr := os.Stat(dir)
	if dirErr != nil && !errors.Is(dirErr, os.ErrNotExist) {
		return nil, gateDiskState{}, dirErr
	}
	if errors.Is(dirErr, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, gateDiskState{}, err
		}
		parent, err := os.Open(filepath.Dir(dir))
		if err != nil {
			return nil, gateDiskState{}, err
		}
		if err := parent.Sync(); err != nil {
			parent.Close()
			return nil, gateDiskState{}, err
		}
		parent.Close()
		j := &gateJournal{path: path}
		if err := j.write(gateDiskState{Status: gateIdle}); err != nil {
			return nil, gateDiskState{}, err
		}
		return j, gateDiskState{Status: gateIdle}, nil
	}
	// An existing journal directory with a missing state file is ambiguous:
	// it may have been deleted after a 429. Fail closed instead of recreating it.
	f, err := os.Open(path)
	if err != nil {
		return nil, gateDiskState{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, gateDiskState{}, err
	}
	if info.Size() > 1024 {
		return nil, gateDiskState{}, fmt.Errorf("invalid provider gate journal size")
	}
	var state gateDiskState
	dec := json.NewDecoder(f)
	if err := dec.Decode(&state); err != nil {
		return nil, gateDiskState{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, gateDiskState{}, fmt.Errorf("invalid provider gate journal trailing data")
	}
	if state.Status != gateIdle && state.Status != gatePending && state.Status != gateBlocked {
		return nil, gateDiskState{}, fmt.Errorf("invalid provider gate state")
	}
	if state.Status == gateBlocked {
		if _, err := time.Parse(time.RFC3339Nano, state.Until); err != nil {
			return nil, gateDiskState{}, fmt.Errorf("invalid provider gate deadline: %w", err)
		}
	}
	return &gateJournal{path: path}, state, nil
}

func (j *gateJournal) write(state gateDiskState) error {
	dir := filepath.Dir(j.path)
	f, err := os.CreateTemp(dir, ".gate-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	data, err := json.Marshal(state)
	if err == nil {
		_, err = f.Write(append(data, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), j.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
