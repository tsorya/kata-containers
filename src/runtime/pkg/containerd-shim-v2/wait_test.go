// Copyright (c) 2026 Red Hat
//
// SPDX-License-Identifier: Apache-2.0
//

package containerdshim

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	eventstypes "github.com/containerd/containerd/api/events"
	vc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers"
	"github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/vcmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitSandboxPublishesWaitAndTaskExitAfterStopBeforeDelete(t *testing.T) {
	stopStarted := make(chan struct{})
	allowStop := make(chan struct{})
	deleteResult := make(chan bool, 2)
	var s *service
	var c *container

	sandbox := &vcmock.Sandbox{
		MockID: testSandboxID,
		StopFunc: func(force bool) error {
			close(stopStarted)
			<-allowStop
			return nil
		},
		DeleteFunc: func() error {
			select {
			case event := <-s.events:
				_, ok := event.(*eventstypes.TaskExit)
				deleteResult <- ok
			default:
				deleteResult <- false
			}
			select {
			case <-c.exitCh:
				deleteResult <- true
			default:
				deleteResult <- false
			}
			return nil
		},
	}

	var err error
	s, err = newService(testSandboxID)
	require.NoError(t, err)
	s.sandbox = sandbox

	c = &container{
		id:       testSandboxID,
		cType:    vc.PodSandbox,
		exitIOch: make(chan struct{}),
		exitCh:   make(chan uint32, 1),
	}
	close(c.exitIOch)

	waitDone := make(chan error, 1)
	go func() {
		_, err := wait(context.Background(), s, c, "")
		waitDone <- err
	}()

	select {
	case <-stopStarted:
	case <-time.After(time.Second):
		t.Fatal("Sandbox.Stop was not called")
	}
	select {
	case <-c.exitCh:
		t.Fatal("wait response published before Sandbox.Stop returned")
	default:
	}
	select {
	case <-s.events:
		t.Fatal("TaskExit published before Sandbox.Stop returned")
	default:
	}

	close(allowStop)

	select {
	case published := <-deleteResult:
		assert.True(t, published, "TaskExit was not published before Sandbox.Delete")
	case <-time.After(time.Second):
		t.Fatal("Sandbox.Delete was not called")
	}
	select {
	case published := <-deleteResult:
		assert.True(t, published, "wait response was not published before Sandbox.Delete")
	case <-time.After(time.Second):
		t.Fatal("Sandbox.Delete did not report TaskExit publication")
	}
	select {
	case err := <-waitDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("wait did not return")
	}
}

func TestWaitSandboxPublishesExitAfterWatcherWinsTeardown(t *testing.T) {
	stopStarted := make(chan struct{})
	releaseStop := make(chan struct{})
	var stopCalls atomic.Int32
	var deleteCalls atomic.Int32

	sandbox := &vcmock.Sandbox{
		MockID: testSandboxID,
		StopFunc: func(force bool) error {
			stopCalls.Add(1)
			close(stopStarted)
			<-releaseStop
			return nil
		},
		DeleteFunc: func() error {
			deleteCalls.Add(1)
			return nil
		},
	}

	s, err := newService(testSandboxID)
	require.NoError(t, err)
	s.sandbox = sandbox

	c := &container{
		id:       testSandboxID,
		cType:    vc.PodSandbox,
		exitIOch: make(chan struct{}),
		exitCh:   make(chan uint32, 1),
	}
	close(c.exitIOch)

	winnerDone := make(chan struct{})
	go func() {
		defer close(winnerDone)
		s.teardownOnce.Do(func() {
			if err := s.sandbox.Stop(context.Background(), true); err != nil {
				t.Errorf("watcher Stop failed: %v", err)
				return
			}
			if err := s.sandbox.Delete(context.Background()); err != nil {
				t.Errorf("watcher Delete failed: %v", err)
			}
		})
	}()

	select {
	case <-stopStarted:
	case <-time.After(time.Second):
		t.Fatal("watcher teardown did not start")
	}

	waitDone := make(chan error, 1)
	go func() {
		_, err := wait(context.Background(), s, c, "")
		waitDone <- err
	}()

	select {
	case <-s.events:
		t.Fatal("TaskExit published before watcher teardown completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseStop)
	select {
	case <-winnerDone:
	case <-time.After(time.Second):
		t.Fatal("watcher teardown did not complete")
	}
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("wait did not complete after watcher teardown")
	}

	assert.Equal(t, int32(1), stopCalls.Load())
	assert.Equal(t, int32(1), deleteCalls.Load())
	select {
	case event := <-s.events:
		_, ok := event.(*eventstypes.TaskExit)
		assert.True(t, ok, "expected TaskExit, got %T", event)
	default:
		t.Fatal("TaskExit was not published")
	}
	select {
	case <-s.events:
		t.Fatal("TaskExit was published more than once")
	default:
	}
}
