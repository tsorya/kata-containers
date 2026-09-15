// Copyright (c) 2026 Red Hat
//
// SPDX-License-Identifier: Apache-2.0
//

package containerdshim

import (
	"context"
	"testing"
	"time"

	vc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers"
	"github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/vcmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitSandboxPublishesExitAfterStopBeforeDelete(t *testing.T) {
	stopStarted := make(chan struct{})
	allowStop := make(chan struct{})
	deleteResult := make(chan bool, 1)
	var s *service

	sandbox := &vcmock.Sandbox{
		MockID: testSandboxID,
		StopFunc: func(force bool) error {
			close(stopStarted)
			<-allowStop
			return nil
		},
		DeleteFunc: func() error {
			select {
			case <-s.ec:
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

	c := &container{
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
	case exitCode := <-c.exitCh:
		assert.Equal(t, uint32(0), exitCode)
	case <-time.After(time.Second):
		t.Fatal("wait response was blocked by Sandbox.Stop")
	}
	select {
	case <-s.ec:
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
	case err := <-waitDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("wait did not return")
	}
}
