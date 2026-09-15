// Copyright (c) 2026 Red Hat
//
// SPDX-License-Identifier: Apache-2.0

package containerdshim

import (
	"context"
	"sync"
	"testing"
	"time"

	eventstypes "github.com/containerd/containerd/api/events"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/containerd/api/types/task"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers"
	"github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/vcmock"
)

type waitResult struct {
	exitCode int32
	err      error
}

func newBlockingSandboxWait(t *testing.T) (*service, *container, *vcmock.Sandbox, <-chan struct{}, func(), <-chan waitResult) {
	t.Helper()

	const (
		sandboxID = "sandbox-id"
		exitCode  = int32(17)
	)

	stopStarted := make(chan struct{})
	releaseStop := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseStop)
		})
	}
	t.Cleanup(release)

	sandbox := &vcmock.Sandbox{
		MockID: sandboxID,
		WaitProcessFunc: func(containerID, processID string) (int32, error) {
			return exitCode, nil
		},
		StopFunc: func(force bool) error {
			close(stopStarted)
			<-releaseStop
			return nil
		},
	}

	s := &service{
		id:         sandboxID,
		hpid:       1234,
		sandbox:    sandbox,
		containers: make(map[string]*container),
		events:     make(chan interface{}, chSize),
		ec:         make(chan exit, bufferSize),
	}
	c := &container{
		s:        s,
		spec:     &specs.Spec{},
		id:       sandboxID,
		cType:    vc.PodSandbox,
		status:   task.Status_RUNNING,
		exitIOch: make(chan struct{}),
		exitCh:   make(chan uint32, 1),
	}
	s.containers[sandboxID] = c
	close(c.exitIOch)

	done := make(chan waitResult, 1)
	go func() {
		ret, err := wait(context.Background(), s, c, "")
		done <- waitResult{exitCode: ret, err: err}
	}()

	return s, c, sandbox, stopStarted, release, done
}

func TestSandboxWaitEnqueuesTaskExitBeforeTeardownCompletes(t *testing.T) {
	s, c, _, stopStarted, releaseStop, waitDone := newBlockingSandboxWait(t)

	select {
	case <-stopStarted:
	case <-time.After(time.Second):
		t.Fatal("sandbox Stop was not called")
	}

	select {
	case event := <-s.events:
		exitEvent, ok := event.(*eventstypes.TaskExit)
		require.True(t, ok, "expected TaskExit, got %T", event)
		assert.Equal(t, c.id, exitEvent.ContainerID)
		assert.Equal(t, c.id, exitEvent.ID)
		assert.Equal(t, uint32(17), exitEvent.ExitStatus)
	case <-time.After(time.Second):
		t.Fatal("TaskExit was not enqueued before sandbox Stop blocked")
	}

	select {
	case result := <-waitDone:
		t.Fatalf("wait returned before sandbox Stop was released: %+v", result)
	default:
	}

	releaseStop()
	select {
	case result := <-waitDone:
		require.NoError(t, result.err)
		assert.Equal(t, int32(17), result.exitCode)
	case <-time.After(time.Second):
		t.Fatal("wait did not return after sandbox Stop was released")
	}
}

func TestSandboxWaitBlocksConcurrentDeleteDuringTeardown(t *testing.T) {
	s, c, sandbox, stopStarted, releaseStop, waitDone := newBlockingSandboxWait(t)

	select {
	case <-stopStarted:
	case <-time.After(time.Second):
		t.Fatal("sandbox Stop was not called")
	}

	sandboxDeleteStarted := make(chan struct{})
	releaseSandboxDelete := make(chan struct{})
	var releaseDeleteOnce sync.Once
	releaseDelete := func() {
		releaseDeleteOnce.Do(func() {
			close(releaseSandboxDelete)
		})
	}
	t.Cleanup(releaseDelete)
	sandbox.DeleteFunc = func() error {
		close(sandboxDeleteStarted)
		<-releaseSandboxDelete
		return nil
	}

	releaseStop()
	select {
	case <-sandboxDeleteStarted:
	case <-time.After(time.Second):
		t.Fatal("sandbox Delete was not called")
	}

	serviceDeleteStarted := make(chan struct{})
	serviceDeleteDone := make(chan error, 1)
	go func() {
		close(serviceDeleteStarted)
		_, err := s.Delete(context.Background(), &taskAPI.DeleteRequest{ID: c.id})
		serviceDeleteDone <- err
	}()
	<-serviceDeleteStarted

	select {
	case err := <-serviceDeleteDone:
		t.Fatalf("service Delete completed while sandbox Delete held the service lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseDelete()

	select {
	case result := <-waitDone:
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		t.Fatal("wait did not return after sandbox Stop was released")
	}

	select {
	case err := <-serviceDeleteDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Delete remained blocked after sandbox teardown released the service lock")
	}
}
