package eventcore

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type testPublishRepo struct {
	getLastSequenceInputs  []PublisherID
	getLastSequenceOutputs []uint64
	getLastSequenceErrs    []error

	getEventsFromSequenceInputs  []uint64
	getEventsFromSequenceLimit   uint64
	getEventsFromSequenceOutputs [][]Event
	getEventsFromSequenceErrs    []error

	saveLastSequenceID     PublisherID
	saveLastSequenceValues []uint64
}

var _ Repository = &testPublishRepo{}

func (r *testPublishRepo) GetLastEvents(uint64) ([]Event, error) {
	panic("GetLastEvents")
}

func (r *testPublishRepo) GetEventsFromSequence(seq uint64, limit uint64) ([]Event, error) {
	index := len(r.getEventsFromSequenceInputs)
	r.getEventsFromSequenceInputs = append(r.getEventsFromSequenceInputs, seq)
	r.getEventsFromSequenceLimit = limit
	return r.getEventsFromSequenceOutputs[index], r.getEventsFromSequenceErrs[index]
}

func (r *testPublishRepo) GetUnprocessedEvents(uint64) ([]Event, error) {
	panic("GetUnprocessedEvents")
}

func (r *testPublishRepo) GetLastSequence(id PublisherID) (uint64, error) {
	index := len(r.getLastSequenceInputs)
	r.getLastSequenceInputs = append(r.getLastSequenceInputs, id)
	return r.getLastSequenceOutputs[index], r.getLastSequenceErrs[index]
}

func (r *testPublishRepo) SaveLastSequence(id PublisherID, seq uint64) error {
	r.saveLastSequenceID = id
	r.saveLastSequenceValues = append(r.saveLastSequenceValues, seq)
	return nil
}

func (r *testPublishRepo) UpdateSequences([]Event) error {
	panic("UpdateSequences")
}

type testPublisher struct {
	inputs [][]Event
	errs   []error
}

var _ Publisher = &testPublisher{}

func (p *testPublisher) GetID() PublisherID {
	return 12
}

func (p *testPublisher) Publish(events []Event) error {
	index := len(p.inputs)
	p.inputs = append(p.inputs, events)
	return p.errs[index]
}

func TestPublisher_GetLastSequence(t *testing.T) {
	repo := &testPublishRepo{
		getLastSequenceOutputs: []uint64{122},
		getLastSequenceErrs:    []error{nil},
	}
	core := NewCore(repo,
		setSequence, getSequence,
		WithErrorTimeout(25*time.Millisecond),
		WithRepositoryLimit(8),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p := &testPublisher{
		errs: []error{nil, nil},
	}

	go func() {
		defer wg.Done()

		core.runPublisher(ctx, p)
	}()

	req := <-core.fetchChan
	assert.Equal(t, uint64(8), req.limit)
	assert.Equal(t, uint64(123), req.fromSequence)
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			testEvent{sequence: 123, num: 1123},
			testEvent{sequence: 124, num: 1124},
		},
	}

	req = <-core.fetchChan
	assert.Equal(t, uint64(8), req.limit)
	assert.Equal(t, uint64(125), req.fromSequence)
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			testEvent{sequence: 125, num: 25},
			testEvent{sequence: 126, num: 26},
		},
	}

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, []PublisherID{12}, repo.getLastSequenceInputs)
	assert.Equal(t, []Event{
		testEvent{sequence: 123, num: 1123},
		testEvent{sequence: 124, num: 1124},
	}, p.inputs[0])
	assert.Equal(t, []Event{
		testEvent{sequence: 125, num: 25},
		testEvent{sequence: 126, num: 26},
	}, p.inputs[1])

	assert.Equal(t, PublisherID(12), repo.saveLastSequenceID)
	assert.Equal(t, []uint64{124, 126}, repo.saveLastSequenceValues)
}

func TestPublisher_GetLastSequenceError(t *testing.T) {
	var logMsg string
	var logErr error
	logger := ErrorLogger(func(message string, err error) {
		logMsg = message
		logErr = err
	})

	repo := &testPublishRepo{
		getLastSequenceOutputs: []uint64{0, 122},
		getLastSequenceErrs:    []error{errors.New("some error"), nil},
	}
	core := NewCore(repo,
		setSequence, getSequence,
		WithErrorTimeout(10*time.Millisecond),
		WithRepositoryLimit(8),
		WithErrorLogger(logger),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p := &testPublisher{}

	go func() {
		defer wg.Done()

		core.runPublisher(ctx, p)
	}()

	time.Sleep(15 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, []PublisherID{12, 12}, repo.getLastSequenceInputs)

	assert.Equal(t, "repo.GetLastSequence", logMsg)
	assert.Equal(t, errors.New("some error"), logErr)
}

func TestPublisher_NotExisted(t *testing.T) {
	repo := &testPublishRepo{
		getLastSequenceOutputs: []uint64{122},
		getLastSequenceErrs:    []error{nil},

		getEventsFromSequenceOutputs: [][]Event{
			{
				testEvent{sequence: 123, num: 23},
				testEvent{sequence: 124, num: 24},
			},
		},
		getEventsFromSequenceErrs: []error{nil},
	}
	core := NewCore(repo,
		setSequence, getSequence,
		WithErrorTimeout(10*time.Millisecond),
		WithRepositoryLimit(8),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p := &testPublisher{
		errs: []error{nil, nil},
	}

	go func() {
		defer wg.Done()

		core.runPublisher(ctx, p)
	}()

	req := <-core.fetchChan
	assert.Equal(t, uint64(8), req.limit)
	assert.Equal(t, uint64(123), req.fromSequence)
	req.responseChan <- fetchResponse{
		existed: false,
	}

	req = <-core.fetchChan
	assert.Equal(t, uint64(8), req.limit)
	assert.Equal(t, uint64(125), req.fromSequence)
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			testEvent{sequence: 125, num: 25},
			testEvent{sequence: 126, num: 26},
		},
	}

	time.Sleep(15 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, []PublisherID{12}, repo.getLastSequenceInputs)
	assert.Equal(t, [][]Event{
		{
			testEvent{sequence: 123, num: 23},
			testEvent{sequence: 124, num: 24},
		},
		{
			testEvent{sequence: 125, num: 25},
			testEvent{sequence: 126, num: 26},
		},
	}, p.inputs)
	assert.Equal(t, []uint64{123}, repo.getEventsFromSequenceInputs)
	assert.Equal(t, uint64(8), repo.getEventsFromSequenceLimit)

	assert.Equal(t, PublisherID(12), repo.saveLastSequenceID)
	assert.Equal(t, []uint64{124, 126}, repo.saveLastSequenceValues)
}
