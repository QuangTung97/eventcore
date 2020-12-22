package eventcore

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestPrepareFetchResponse(t *testing.T) {
	table := []struct {
		name          string
		events        []Event
		req           fetchRequest
		bufferSize    uint64
		sequence      uint64
		firstSequence uint64

		expected fetchResponse
	}{
		{
			name: "simple",
			events: []Event{
				0, 2, 3, 4,
			},
			bufferSize:    4,
			sequence:      3,
			firstSequence: 1,
			req: fetchRequest{
				fromSequence: 1,
				limit:        2,
			},

			expected: fetchResponse{
				existed: true,
				result:  []Event{2, 3},
			},
		},
		{
			name: "simple",
			events: []Event{
				0, 2, 3, 4,
			},
			bufferSize:    4,
			sequence:      3,
			firstSequence: 1,
			req: fetchRequest{
				fromSequence: 3,
				limit:        2,
			},

			expected: fetchResponse{
				existed: true,
				result:  []Event{4},
			},
		},
		{
			name: "less than last sequence",
			events: []Event{
				9, 0, 0, 4, 5, 6, 7, 8,
			},
			bufferSize:    8,
			sequence:      9,
			firstSequence: 3,
			req: fetchRequest{
				fromSequence: 1,
				limit:        2,
			},

			expected: fetchResponse{
				existed: false,
			},
		},
		{
			name: "wrap around",
			events: []Event{
				9, 10, 0, 4, 5, 6, 7, 8,
			},
			bufferSize:    8,
			sequence:      9,
			firstSequence: 3,
			req: fetchRequest{
				fromSequence: 6,
				limit:        4,
			},

			expected: fetchResponse{
				existed: true,
				result:  []Event{7, 8, 9, 10},
			},
		},
		{
			name: "overlapped",
			events: []Event{
				9, 10, 11, 8,
			},
			bufferSize:    4,
			sequence:      6,
			firstSequence: 1,
			req: fetchRequest{
				fromSequence: 2,
				limit:        2,
			},

			expected: fetchResponse{
				existed: false,
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			res := prepareFetchResponse(e.events, e.req, e.sequence, e.firstSequence, e.bufferSize)
			assert.Equal(t, e.expected, res)
		})
	}
}

type testRepo struct {
	getLastEventsCount         int
	getEventsFromSequenceCount int
	getUnprocessedEventsCount  int
	getLastSequenceCount       int
	saveLastSequenceCount      int
	updateSequencesCount       int

	unprocessedEvents []Event
	updatedEvents     []Event
}

var _ Repository = &testRepo{}

func (r *testRepo) GetLastEvents(limit uint64) ([]Event, error) {
	r.getLastEventsCount++
	return nil, nil
}

func (r *testRepo) GetEventsFromSequence(seq uint64, limit uint64) ([]Event, error) {
	r.getEventsFromSequenceCount++
	return nil, nil
}

func (r *testRepo) GetUnprocessedEvents(limit uint64) ([]Event, error) {
	r.getUnprocessedEventsCount++
	return r.unprocessedEvents, nil
}

func (r *testRepo) GetLastSequence(id PublisherID) (uint64, error) {
	r.getLastSequenceCount++
	return 0, nil
}

func (r *testRepo) SaveLastSequence(id PublisherID, seq uint64) error {
	r.saveLastSequenceCount++
	return nil
}

func (r *testRepo) UpdateSequences(events []Event) error {
	r.updateSequencesCount++
	r.updatedEvents = events
	return nil
}

type testEvent struct {
	sequence uint64
	num      int
}

func TestRunDBProcessor(t *testing.T) {
	unprocessedEvents := []Event{
		testEvent{num: 20},
		testEvent{num: 21},
		testEvent{num: 22},
	}

	repo := &testRepo{
		unprocessedEvents: unprocessedEvents,
	}
	core := NewCore(repo,
		func(e Event, seq uint64) Event {
			event := e.(testEvent)
			event.sequence = seq
			return event
		},
		func(e Event) uint64 {
			return e.(testEvent).sequence
		},
		WithErrorTimeout(100*time.Millisecond),
		WithRepositoryLimit(8),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var events []Event

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := core.runDBProcessor(ctx, events)
		if err != nil {
			fmt.Println(err)
		}
	}()

	core.Signal()
	core.Signal()
	core.Signal()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	updatedEvents := []Event{
		testEvent{sequence: 1, num: 20},
		testEvent{sequence: 2, num: 21},
		testEvent{sequence: 3, num: 22},
	}

	expected := &testRepo{
		getUnprocessedEventsCount: 1,
		updateSequencesCount:      1,
		unprocessedEvents:         unprocessedEvents,
		updatedEvents:             updatedEvents,
	}

	assert.Equal(t, expected, repo)

	var listenEvents []Event
	listenEvents = append(listenEvents, <-core.listenChan)
	listenEvents = append(listenEvents, <-core.listenChan)
	listenEvents = append(listenEvents, <-core.listenChan)

	assert.Equal(t, updatedEvents, listenEvents)
}

func TestRunDBProcessorWithLastEvents(t *testing.T) {
	unprocessedEvents := []Event{
		testEvent{num: 20},
		testEvent{num: 21},
		testEvent{num: 22},
	}

	repo := &testRepo{
		unprocessedEvents: unprocessedEvents,
	}
	core := NewCore(repo,
		func(e Event, seq uint64) Event {
			event := e.(testEvent)
			event.sequence = seq
			return event
		},
		func(e Event) uint64 {
			return e.(testEvent).sequence
		},
		WithErrorTimeout(25*time.Millisecond),
		WithRepositoryLimit(8),
	)

	assert.Equal(t, 25*time.Millisecond, core.errorTimeout)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := []Event{
		testEvent{sequence: 2, num: 50},
		testEvent{sequence: 3, num: 50},
		testEvent{sequence: 4, num: 50},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := core.runDBProcessor(ctx, events)
		if err != nil {
			fmt.Println(err)
		}
	}()

	time.Sleep(30 * time.Millisecond)

	cancel()
	wg.Wait()

	updatedEvents := []Event{
		testEvent{sequence: 5, num: 20},
		testEvent{sequence: 6, num: 21},
		testEvent{sequence: 7, num: 22},
	}

	expected := &testRepo{
		getUnprocessedEventsCount: 1,
		updateSequencesCount:      1,
		unprocessedEvents:         unprocessedEvents,
		updatedEvents:             updatedEvents,
	}

	assert.Equal(t, expected, repo)

	var listenEvents []Event
	listenEvents = append(listenEvents, <-core.listenChan)
	listenEvents = append(listenEvents, <-core.listenChan)
	listenEvents = append(listenEvents, <-core.listenChan)

	assert.Equal(t, updatedEvents, listenEvents)
}

func TestRunListener(t *testing.T) {
	repo := &testRepo{}
	core := NewCore(repo,
		func(e Event, seq uint64) Event {
			event := e.(testEvent)
			event.sequence = seq
			return event
		},
		func(e Event) uint64 {
			return e.(testEvent).sequence
		},
		WithErrorTimeout(25*time.Millisecond),
		WithRepositoryLimit(8),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := []Event{
		testEvent{sequence: 2, num: 52},
		testEvent{sequence: 3, num: 53},
		testEvent{sequence: 4, num: 54},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		core.runListener(ctx, events)
	}()

	ch := make(chan fetchResponse)

	core.fetch(fetchRequest{
		limit:        4,
		fromSequence: 3,
		responseChan: ch,
	})

	time.Sleep(10 * time.Millisecond)
	res := <-ch

	ch2 := make(chan fetchResponse)
	core.fetch(fetchRequest{
		fromSequence: 5,
		limit:        4,
		responseChan: ch2,
	})

	time.Sleep(10 * time.Millisecond)

	core.listenChan <- testEvent{
		sequence: 5,
		num:      55,
	}

	time.Sleep(10 * time.Millisecond)
	res2 := <-ch2

	cancel()
	wg.Wait()

	assert.True(t, res.existed)
	assert.Equal(t, []Event{
		testEvent{sequence: 3, num: 53},
		testEvent{sequence: 4, num: 54},
	}, res.result)

	assert.True(t, res2.existed)
	assert.Equal(t, []Event{
		testEvent{sequence: 5, num: 55},
	}, res2.result)
}
