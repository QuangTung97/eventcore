package eventcore_test

import (
	"context"
	"errors"
	"github.com/QuangTung97/eventcore"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type getOutput struct {
	events []eventcore.Event
	err    error
}

type getEventsFromSequenceInput struct {
	seq   uint64
	limit uint64
}

type getLastSequenceOutput struct {
	sequence uint64
	err      error
}

type saveLastSequenceInput struct {
	id  eventcore.PublisherID
	seq uint64
}

type updateSeqInput struct {
	events []eventcore.Event
}

type testRepo struct {
	getLastInputs  []uint64
	getLastOutputs []getOutput

	getFromSequenceInputs  []getEventsFromSequenceInput
	getFromSequenceOutputs []getOutput

	getUnprocessedInputs  []uint64
	getUnprocessedOutputs []getOutput

	getLastSequenceInputs  []eventcore.PublisherID
	getLastSequenceOutputs []getLastSequenceOutput

	saveLastSequenceCalled bool
	saveLastInput          saveLastSequenceInput
	saveLastOutput         error

	updateInputs  []updateSeqInput
	updateOutputs []error
}

var _ eventcore.Repository = &testRepo{}

func (r *testRepo) GetLastEvents(limit uint64) ([]eventcore.Event, error) {
	index := len(r.getLastInputs)
	r.getLastInputs = append(r.getLastInputs, limit)
	return r.getLastOutputs[index].events, r.getLastOutputs[index].err
}

func (r *testRepo) GetEventsFromSequence(seq uint64, limit uint64) ([]eventcore.Event, error) {
	index := len(r.getFromSequenceInputs)
	r.getFromSequenceInputs = append(r.getFromSequenceInputs, getEventsFromSequenceInput{
		seq:   seq,
		limit: limit,
	})
	out := r.getFromSequenceOutputs[index]
	return out.events, out.err
}

func (r *testRepo) GetUnprocessedEvents(limit uint64) ([]eventcore.Event, error) {
	index := len(r.getUnprocessedInputs)
	r.getUnprocessedInputs = append(r.getUnprocessedInputs, limit)
	out := r.getUnprocessedOutputs[index]
	return out.events, out.err
}

func (r *testRepo) GetLastSequence(id eventcore.PublisherID) (uint64, error) {
	index := len(r.getLastSequenceInputs)
	r.getLastSequenceInputs = append(r.getLastSequenceInputs, id)
	out := r.getLastSequenceOutputs[index]
	return out.sequence, out.err
}

func (r *testRepo) SaveLastSequence(id eventcore.PublisherID, seq uint64) error {
	r.saveLastInput = saveLastSequenceInput{
		id:  id,
		seq: seq,
	}
	r.saveLastSequenceCalled = true
	return r.saveLastOutput
}

func (r *testRepo) UpdateSequences(events []eventcore.Event) error {
	index := len(r.updateInputs)
	r.updateInputs = append(r.updateInputs, updateSeqInput{
		events: events,
	})
	err := r.updateOutputs[index]
	return err
}

type testEvent struct {
	sequence uint64
	num      int
}

func getSequence(e eventcore.Event) uint64 {
	return e.(testEvent).sequence
}

func setSequence(e eventcore.Event, seq uint64) eventcore.Event {
	return testEvent{
		sequence: seq,
		num:      e.(testEvent).num,
	}
}

type testLogger struct {
	messages []string
	errs     []error
}

func (l *testLogger) logError(msg string, err error) {
	l.messages = append(l.messages, msg)
	l.errs = append(l.errs, err)
}

type testPublisher struct {
	id              eventcore.PublisherID
	publishedEvents []eventcore.Event
	publishOutput   error
}

var _ eventcore.Publisher = &testPublisher{}

func (p *testPublisher) GetID() eventcore.PublisherID {
	return p.id
}

func (p *testPublisher) Publish(events []eventcore.Event) error {
	p.publishedEvents = append(p.publishedEvents, events...)
	return p.publishOutput
}

type testAsyncPublisher struct {
	id              eventcore.PublisherID
	publishedEvents []eventcore.Event
	publishOutput   error
	ch              chan eventcore.CommittedEvent
}

var _ eventcore.AsyncPublisher = &testAsyncPublisher{}

func (p *testAsyncPublisher) GetID() eventcore.PublisherID {
	return p.id
}

func (p *testAsyncPublisher) PublishAsync(events []eventcore.Event) error {
	p.publishedEvents = append(p.publishedEvents, events...)
	return p.publishOutput
}

func (p *testAsyncPublisher) GetCommitChannel() chan eventcore.CommittedEvent {
	return p.ch
}

func TestCore_RunNoSignal(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,
	}, repo)
	assert.Equal(t, &testLogger{}, logger)
}

func TestCore_RunWithSignal(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 20},
					testEvent{sequence: 0, num: 21},
					testEvent{sequence: 0, num: 22},
					testEvent{sequence: 0, num: 23},
				},
			},
		},
		updateOutputs: []error{nil},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedInputs:  []uint64{9},
		getUnprocessedOutputs: repo.getUnprocessedOutputs,

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 1, num: 20},
					testEvent{sequence: 2, num: 21},
					testEvent{sequence: 3, num: 22},
					testEvent{sequence: 4, num: 23},
				},
			},
		},
		updateOutputs: repo.updateOutputs,
	}, repo)
	assert.Equal(t, &testLogger{}, logger)
}

func TestCore_RunWithSignalAndExistingEvents(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 100, num: 10},
					testEvent{sequence: 101, num: 11},
				},
				err: nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 20},
					testEvent{sequence: 0, num: 21},
					testEvent{sequence: 0, num: 22},
					testEvent{sequence: 0, num: 23},
				},
			},
		},
		updateOutputs: []error{nil},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	core.Signal()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedInputs:  []uint64{9},
		getUnprocessedOutputs: repo.getUnprocessedOutputs,

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 102, num: 20},
					testEvent{sequence: 103, num: 21},
					testEvent{sequence: 104, num: 22},
					testEvent{sequence: 105, num: 23},
				},
			},
		},
		updateOutputs: repo.updateOutputs,
	}, repo)
	assert.Equal(t, &testLogger{}, logger)
}

func TestCore_RunGetLastEventsError(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    errors.New("get last error"),
			},
		},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,
	}, repo)
	assert.Equal(t, &testLogger{
		messages: []string{
			"repo.GetLastEvents",
		},
		errs: []error{
			errors.New("get last error"),
		},
	}, logger)
}

func TestCore_RunGetUnprocessedError(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				err: errors.New("get unprocessed"),
			},
		},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	core.Signal()

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedOutputs: repo.getUnprocessedOutputs,
		getUnprocessedInputs:  []uint64{9},
	}, repo)
	assert.Equal(t, &testLogger{
		messages: []string{
			"c.runDBProcessor",
		},
		errs: []error{
			errors.New("get unprocessed"),
		},
	}, logger)
}

func TestCore_RunGetUnprocessedEmpty(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 101},
					testEvent{sequence: 0, num: 102},
					testEvent{sequence: 0, num: 103},
					testEvent{sequence: 0, num: 104},
					testEvent{sequence: 0, num: 105},
				},
				err: nil,
			},
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 105},
					testEvent{sequence: 0, num: 106},
				},
				err: nil,
			},
		},
		updateOutputs: []error{nil, nil},
	}

	logger := &testLogger{}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(4),
		eventcore.WithErrorTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	time.Sleep(10 * time.Millisecond)

	core.Signal()
	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{4},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedOutputs: repo.getUnprocessedOutputs,
		getUnprocessedInputs:  []uint64{5, 5, 5},

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 1, num: 101},
					testEvent{sequence: 2, num: 102},
					testEvent{sequence: 3, num: 103},
					testEvent{sequence: 4, num: 104},
				},
			},
			{
				events: []eventcore.Event{
					testEvent{sequence: 5, num: 105},
					testEvent{sequence: 6, num: 106},
				},
			},
		},
		updateOutputs: repo.updateOutputs,
	}, repo)
	assert.Equal(t, &testLogger{}, logger)
}

func TestCore_RunPublisher(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 101},
					testEvent{sequence: 0, num: 102},
					testEvent{sequence: 0, num: 103},
					testEvent{sequence: 0, num: 104},
					testEvent{sequence: 0, num: 105},
				},
				err: nil,
			},
		},
		updateOutputs: []error{nil},
		getLastSequenceOutputs: []getLastSequenceOutput{
			{
				sequence: 0,
				err:      nil,
			},
		},
		saveLastOutput: nil,
	}

	logger := &testLogger{}

	publisher := &testPublisher{
		id:            222,
		publishOutput: nil,
	}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
		eventcore.AddPublisher(publisher),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedOutputs: repo.getUnprocessedOutputs,
		getUnprocessedInputs:  []uint64{9},

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 1, num: 101},
					testEvent{sequence: 2, num: 102},
					testEvent{sequence: 3, num: 103},
					testEvent{sequence: 4, num: 104},
					testEvent{sequence: 5, num: 105},
				},
			},
		},
		updateOutputs: repo.updateOutputs,

		getLastSequenceInputs: []eventcore.PublisherID{
			222,
		},
		getLastSequenceOutputs: repo.getLastSequenceOutputs,

		saveLastInput: saveLastSequenceInput{
			id:  222,
			seq: 5,
		},
		saveLastSequenceCalled: true,
		saveLastOutput:         repo.saveLastOutput,
	}, repo)

	assert.Equal(t, &testLogger{}, logger)

	assert.Equal(t, &testPublisher{
		id:            publisher.id,
		publishOutput: publisher.publishOutput,
		publishedEvents: []eventcore.Event{
			testEvent{
				sequence: 1,
				num:      101,
			},
			testEvent{
				sequence: 2,
				num:      102,
			},
			testEvent{
				sequence: 3,
				num:      103,
			},
			testEvent{
				sequence: 4,
				num:      104,
			},
			testEvent{
				sequence: 5,
				num:      105,
			},
		},
	}, publisher)
}

func TestCore_RunAsyncPublisher(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 101},
					testEvent{sequence: 0, num: 102},
					testEvent{sequence: 0, num: 103},
					testEvent{sequence: 0, num: 104},
					testEvent{sequence: 0, num: 105},
				},
				err: nil,
			},
		},
		updateOutputs: []error{nil},
		getLastSequenceOutputs: []getLastSequenceOutput{
			{
				sequence: 0,
				err:      nil,
			},
		},
		saveLastOutput: nil,
	}

	logger := &testLogger{}

	publisher := &testAsyncPublisher{
		id:            333,
		publishOutput: nil,
		ch:            make(chan eventcore.CommittedEvent),
	}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
		eventcore.AddAsyncPublisher(publisher),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	time.Sleep(10 * time.Millisecond)

	publisher.ch <- eventcore.CommittedEvent{
		Sequence: 5,
		Error:    nil,
	}

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedOutputs: repo.getUnprocessedOutputs,
		getUnprocessedInputs:  []uint64{9},

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 1, num: 101},
					testEvent{sequence: 2, num: 102},
					testEvent{sequence: 3, num: 103},
					testEvent{sequence: 4, num: 104},
					testEvent{sequence: 5, num: 105},
				},
			},
		},
		updateOutputs: repo.updateOutputs,

		getLastSequenceInputs: []eventcore.PublisherID{
			333,
		},
		getLastSequenceOutputs: repo.getLastSequenceOutputs,

		saveLastInput: saveLastSequenceInput{
			id:  333,
			seq: 5,
		},
		saveLastSequenceCalled: true,
		saveLastOutput:         repo.saveLastOutput,
	}, repo)

	assert.Equal(t, &testLogger{}, logger)

	assert.Equal(t, &testAsyncPublisher{
		id:            publisher.id,
		ch:            publisher.ch,
		publishOutput: publisher.publishOutput,
		publishedEvents: []eventcore.Event{
			testEvent{
				sequence: 1,
				num:      101,
			},
			testEvent{
				sequence: 2,
				num:      102,
			},
			testEvent{
				sequence: 3,
				num:      103,
			},
			testEvent{
				sequence: 4,
				num:      104,
			},
			testEvent{
				sequence: 5,
				num:      105,
			},
		},
	}, publisher)
}

func TestCore_RunAsyncPublisher_CommitError(t *testing.T) {
	repo := &testRepo{
		getLastOutputs: []getOutput{
			{
				events: nil,
				err:    nil,
			},
		},
		getUnprocessedOutputs: []getOutput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 0, num: 101},
					testEvent{sequence: 0, num: 102},
					testEvent{sequence: 0, num: 103},
					testEvent{sequence: 0, num: 104},
					testEvent{sequence: 0, num: 105},
				},
				err: nil,
			},
		},
		updateOutputs: []error{nil},
		getLastSequenceOutputs: []getLastSequenceOutput{
			{
				sequence: 0,
				err:      nil,
			},
		},
		saveLastOutput: nil,
	}

	logger := &testLogger{}

	publisher := &testAsyncPublisher{
		id:            333,
		publishOutput: nil,
		ch:            make(chan eventcore.CommittedEvent),
	}

	core := eventcore.NewCore(repo,
		setSequence, getSequence,
		eventcore.WithErrorLogger(logger.logError),
		eventcore.WithRepositoryLimit(8),
		eventcore.WithErrorTimeout(50*time.Millisecond),
		eventcore.AddAsyncPublisher(publisher),
	)

	var wg sync.WaitGroup

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)

	go func() {
		defer wg.Done()

		core.Run(ctx)
	}()

	core.Signal()
	time.Sleep(10 * time.Millisecond)

	publisher.ch <- eventcore.CommittedEvent{
		Sequence: 0,
		Error:    errors.New("some commit error"),
	}

	time.Sleep(10 * time.Millisecond)

	cancel()
	wg.Wait()

	assert.Equal(t, &testRepo{
		getLastInputs:  []uint64{8},
		getLastOutputs: repo.getLastOutputs,

		getUnprocessedOutputs: repo.getUnprocessedOutputs,
		getUnprocessedInputs:  []uint64{9},

		updateInputs: []updateSeqInput{
			{
				events: []eventcore.Event{
					testEvent{sequence: 1, num: 101},
					testEvent{sequence: 2, num: 102},
					testEvent{sequence: 3, num: 103},
					testEvent{sequence: 4, num: 104},
					testEvent{sequence: 5, num: 105},
				},
			},
		},
		updateOutputs: repo.updateOutputs,

		getLastSequenceInputs: []eventcore.PublisherID{
			333,
		},
		getLastSequenceOutputs: repo.getLastSequenceOutputs,

		saveLastInput: saveLastSequenceInput{
			id:  0,
			seq: 0,
		},
		saveLastSequenceCalled: false,
		saveLastOutput:         repo.saveLastOutput,
	}, repo)

	assert.Equal(t, &testLogger{
		messages: []string{"Commit channel"},
		errs:     []error{errors.New("some commit error")},
	}, logger)

	assert.Equal(t, &testAsyncPublisher{
		id:            publisher.id,
		ch:            publisher.ch,
		publishOutput: publisher.publishOutput,
		publishedEvents: []eventcore.Event{
			testEvent{
				sequence: 1,
				num:      101,
			},
			testEvent{
				sequence: 2,
				num:      102,
			},
			testEvent{
				sequence: 3,
				num:      103,
			},
			testEvent{
				sequence: 4,
				num:      104,
			},
			testEvent{
				sequence: 5,
				num:      105,
			},
		},
	}, publisher)
}
