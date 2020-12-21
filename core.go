package eventcore

import (
	"context"
	"sync"
	"time"

	"github.com/cheekybits/genny/generic"
)

// Event ...
type Event generic.Type

// GetSequence ...
type GetSequence func(e Event) uint64

// SetSequence ...
type SetSequence func(e Event, seq uint64) Event

// PublisherID ...
type PublisherID uint32

// Repository ...
type Repository interface {
	GetLastEvents(limit uint64) ([]Event, error)
	GetEventsFromSequence(seq uint64, limit uint64) ([]Event, error)
	GetUnprocessedEvents(limit uint64) ([]Event, error)

	GetLastSequence(id PublisherID) (uint64, error)
	SaveLastSequence(id PublisherID, seq uint64) error

	UpdateSequences(events []Event) error
}

// Publisher ...
type Publisher interface {
	GetID() PublisherID
	Publish(events []Event) error
}

type fetchRequest struct {
	limit        uint64
	fromSequence uint64
	result       []Event
	responseChan chan fetchResponse
}

type fetchResponse struct {
	existed bool
	result  []Event
}

// Core ...
type Core struct {
	repo           Repository
	sequenceSetter SetSequence
	sequenceGetter GetSequence

	signalChan chan struct{}
	listenChan chan Event
	fetchChan  chan fetchRequest

	// options
	repoLimit    uint64
	errorTimeout time.Duration

	publishers []Publisher
}

// NewCore ...
func NewCore(
	repo Repository,
	setter SetSequence, getter GetSequence,
	options ...Option,
) *Core {
	opts := defaultCoreOpts
	applyOptions(opts, options...)

	return &Core{
		repo:           repo,
		sequenceSetter: setter,
		sequenceGetter: getter,

		signalChan: make(chan struct{}, opts.repoLimit),
		listenChan: make(chan Event, opts.repoLimit),
		fetchChan:  make(chan fetchRequest, opts.fetchLimit),

		repoLimit:    opts.repoLimit,
		errorTimeout: opts.errorTimeout,

		publishers: opts.publishers,
	}
}

func (c *Core) runDBProcessor(ctx context.Context, lastEvents []Event) error {
	lastSequence := uint64(0)
	if len(lastEvents) > 0 {
		lastSequence = c.sequenceGetter(lastEvents[len(lastEvents)-1])
	}

	for {
		signalCount := uint64(0)

		select {
		case <-c.signalChan:
			break
		case <-time.After(c.errorTimeout):
			break
		case <-ctx.Done():
			return nil
		}
		signalCount++

		// drain all signals
	DrainLoop:
		for ; signalCount < c.repoLimit; signalCount++ {
			select {
			case <-c.signalChan:
				continue DrainLoop
			default:
				break DrainLoop
			}
		}

		events, err := c.repo.GetUnprocessedEvents(c.repoLimit)
		if err != nil {
			return err
		}

		for i := range events {
			events[i] = c.sequenceSetter(events[i], lastSequence+uint64(i)+1)
		}
		lastSequence += uint64(len(events))

		err = c.repo.UpdateSequences(events)
		if err != nil {
			return err
		}

		for _, e := range events {
			c.listenChan <- e
		}
	}
}

func prepareFetchResponse(
	events []Event, req fetchRequest,
	sequence uint64, firstSequence uint64,
	bufferSize uint64,
) fetchResponse {
	if req.fromSequence < firstSequence {
		return fetchResponse{
			existed: false,
		}
	}

	if req.fromSequence+bufferSize < sequence+1 {
		return fetchResponse{
			existed: false,
		}
	}

	result := req.result

	top := sequence + 1
	if top > req.fromSequence+req.limit {
		top = req.fromSequence + req.limit
	}

	last := top % bufferSize
	first := req.fromSequence % bufferSize

	if last >= first {
		result = append(result, events[first:last]...)
	} else {
		result = append(result, events[first:]...)
		result = append(result, events[:last]...)
	}

	return fetchResponse{
		existed: true,
		result:  result,
	}
}

func (c *Core) runListener(ctx context.Context, lastEvents []Event) {
	bufferSize := c.repoLimit
	events := make([]Event, c.repoLimit)

	waitingFetches := make([]fetchRequest, 0, 100)

	for _, e := range lastEvents {
		index := c.sequenceGetter(e) % bufferSize
		events[index] = e
	}

	sequence := uint64(0)
	firstSequence := uint64(1)
	if len(lastEvents) > 0 {
		n := len(lastEvents)
		sequence = c.sequenceGetter(lastEvents[n-1])
		firstSequence = c.sequenceGetter(lastEvents[0])
	}

	for {
		select {
		case event := <-c.listenChan:
			sequence = c.sequenceGetter(event)
			index := sequence % bufferSize
			events[index] = event

			for _, req := range waitingFetches {
				res := prepareFetchResponse(events, req, sequence, firstSequence, bufferSize)
				req.responseChan <- res
			}
			waitingFetches = waitingFetches[:0]

		case req := <-c.fetchChan:
			if req.fromSequence > sequence+1 {
				panic("req.fromSequence > sequence + 1")
			}
			if req.fromSequence == sequence+1 {
				waitingFetches = append(waitingFetches, req)
			} else {
				res := prepareFetchResponse(events, req, sequence, firstSequence, bufferSize)
				req.responseChan <- res
			}

		case <-ctx.Done():
			return
		}
	}
}

func (c *Core) runLoop(ctx context.Context) {
	lastEvents, err := c.repo.GetLastEvents(c.repoLimit)
	if err != nil {
		// TODO logging error
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		err := c.runDBProcessor(ctx, lastEvents)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// TODO logging
			cancel()
		}
	}()

	go func() {
		defer wg.Done()

		c.runListener(ctx, lastEvents)
	}()

	wg.Wait()
}

// Run ...
func (c *Core) Run(ctx context.Context) {
	for {
		c.runLoop(ctx)
		if ctx.Err() != nil {
			return
		}
		time.Sleep(c.errorTimeout)
	}
}

// Signal ...
func (c *Core) Signal() {
	c.signalChan <- struct{}{}
}

func (c *Core) fetch(req fetchRequest) {
	c.fetchChan <- req
}
