package boltqueue

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setup(t assert.TestingT) (*PQueue, func()) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueue(queueFile)
	if !assert.NoError(t, err) {
		panic(err)
	}
	return testPQueue, func() {
		testPQueue.Close()
		os.Remove(queueFile)
	}
}

func TestNoDeadLockInSize(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()
	n, err := testPQueue.Size(0)
	assert.NoError(t, err)
	assert.Zero(t, n)

	err = testPQueue.Enqueue(0, NewMessage("hello"))
	assert.NoError(t, err)
	err = testPQueue.Enqueue(0, NewMessage("world")) // deadlock here in commit 944e945c0fdf6
	assert.NoError(t, err)
}

func TestDequeueEmptyMessage(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()

	m, err := testPQueue.Dequeue()
	assert.NoError(t, err)
	assert.Nil(t, m)
}

func TestEnqueue(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()

	// Enqueue 50 messages
	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 10 {
			t.Errorf("Expected queue size 10 for priority %d. Got: %d", p, s)
		}
	}
}

func TestDequeue(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()

	//Put them in in reverse priority order
	for p := 5; p >= 1; p-- {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			mStrComp := fmt.Sprintf("test message %d-%d", p, n)
			m, err := testPQueue.Dequeue()
			if err != nil {
				t.Error("Error dequeueing:", err)
			}
			mStr := m.ToString()
			if mStr != mStrComp {
				t.Errorf("Expected message: \"%s\" got: \"%s\"", mStrComp, mStr)
			}
			if m.Priority() != p {
				t.Errorf("Expected priority: %d, got: %d", p, m.Priority())
			}
		}
	}
	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 0 {
			t.Errorf("Expected queue size 0 for priority %d. Got: %d", p, s)
		}
	}
}

func TestRequeue(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()

	for p := 1; p <= 5; p++ {
		err := testPQueue.Enqueue(p, NewMessage(fmt.Sprintf("test message %d", p)))
		if err != nil {
			t.Error(err)
		}
	}
	mp1, err := testPQueue.Dequeue()
	if err != nil {
		t.Error(err)
	}
	//Remove the priority 2 message
	_, _ = testPQueue.Dequeue()

	//Re-enqueue the message at priority 1
	err = testPQueue.RequeueAs(1, mp1)
	if err != nil {
		t.Error(err)
	}

	//And it should be the first to emerge
	mp1, err = testPQueue.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if mp1.ToString() != "test message 1" {
		t.Errorf("Expected: \"%s\", got: \"%s\"", "test message 1", mp1.ToString())
	}

}

func TestGoroutines(t *testing.T) {
	testPQueue, done := setup(t)
	defer done()

	var wg sync.WaitGroup

	for g := 1; g <= 5; g++ {
		wg.Add(1)
		go func() {
			rand.Seed(time.Now().Unix())
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			for p := 1; p <= 5; p++ {
				for n := 1; n <= 2; n++ {
					err := testPQueue.Enqueue(p, NewMessage(fmt.Sprintf("test message %d", p)))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 10 {
			t.Errorf("Expected queue size 10 for priority %d. Got: %d", p, s)
		}
	}

	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			mStrComp := fmt.Sprintf("test message %d", p)
			m, err := testPQueue.Dequeue()
			if err != nil {
				t.Error("Error dequeueing:", err)
			}
			mStr := m.ToString()
			if mStr != mStrComp {
				t.Errorf("Expected message: \"%s\" got: \"%s\"", mStrComp, mStr)
			}
			if m.Priority() != p {
				t.Errorf("Expected priority: %d, got: %d", p, m.Priority())
			}
		}
	}
	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 0 {
			t.Errorf("Expected queue size 0 for priority %d. Got: %d", p, s)
		}
	}
}

func BenchmarkPQueue(b *testing.B) {
	b.StopTimer()
	queue, done := setup(b)
	defer done()

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for p := 1; p <= 5; p++ {
			err := queue.Enqueue(p, NewMessage("test message"))
			assert.NoError(b, err)
		}
	}

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue()
			assert.NoError(b, err)
		}
	}
	b.StopTimer()
}

func BenchmarkPQueue_PriorityEnqueue(b *testing.B) {
	b.StopTimer()
	queue, done := setup(b)
	defer done()

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for p := 1; p <= 5; p++ {
			err := queue.Enqueue(p, NewMessage("test message"))
			assert.NoError(b, err)
		}
	}
	b.StopTimer()
}

func BenchmarkPQueue_PriorityDequeue(b *testing.B) {
	b.StopTimer()
	queue, done := setup(b)
	defer done()

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		for p := 1; p <= 6; p++ {
			err := queue.Enqueue(p, NewMessage("test message"))
			assert.NoError(b, err)
		}
		b.StartTimer()
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue()
			assert.NoError(b, err)
		}
	}
	b.StopTimer()
}

func BenchmarkPQueue_Enqueue(b *testing.B) {
	b.StopTimer()
	queue, done := setup(b)
	defer done()

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		err := queue.Enqueue(0, NewMessage("test message"))
		assert.NoError(b, err)
	}
	b.StopTimer()
}

func BenchmarkPQueue_Dequeue(b *testing.B) {
	b.StopTimer()
	queue, done := setup(b)
	defer done()

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		err := queue.Enqueue(0, NewMessage("test message"))
		assert.NoError(b, err)
		b.StartTimer()

		_, err = queue.Dequeue()
		assert.NoError(b, err)
	}
	b.StopTimer()
}
