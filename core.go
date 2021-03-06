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

// CommittedEvent ...
type CommittedEvent struct {
	Sequence uint64
	Error    error
}

// AsyncPublisher ...
type AsyncPublisher interface {
	GetID() PublisherID
	PublishAsync(events []Event) error
	GetCommitChannel() chan CommittedEvent
}

// Observer ...
type Observer struct {
	sequence       uint64
	core           *Core
	reservedEvents []Event
	responseChan   chan fetchResponse
}

// ErrorLogger ...
type ErrorLogger func(message string, err error)

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

	publishers      []Publisher
	asyncPublishers []AsyncPublisher

	logger ErrorLogger
}

// NewCore ...
func NewCore(
	repo Repository,
	setter SetSequence, getter GetSequence,
	options ...Option,
) *Core {
	opts := defaultCoreOpts
	applyOptions(&opts, options...)

	return &Core{
		repo:           repo,
		sequenceSetter: setter,
		sequenceGetter: getter,

		signalChan: make(chan struct{}, opts.repoLimit),
		listenChan: make(chan Event, opts.repoLimit),
		fetchChan:  make(chan fetchRequest, opts.fetchLimit),

		repoLimit:    opts.repoLimit,
		errorTimeout: opts.errorTimeout,

		publishers:      opts.publishers,
		asyncPublishers: opts.asyncPublishers,

		logger: opts.logger,
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

		events, err := c.repo.GetUnprocessedEvents(c.repoLimit + 1)
		if err != nil {
			return err
		}

		if len(events) == 0 {
			continue
		}

		if uint64(len(events)) > c.repoLimit {
			select {
			case c.signalChan <- struct{}{}:
				break
			default:
				break
			}
			events = events[:c.repoLimit]
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

	if last > first {
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

func (c *Core) runPublisher(ctx context.Context, p Publisher) {
	var lastSequence uint64
	for {
		var err error
		lastSequence, err = c.repo.GetLastSequence(p.GetID())
		if err != nil {
			c.logger("repo.GetLastSequence", err)
			sleepContext(ctx, c.errorTimeout)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		break
	}

	reservedEvents := make([]Event, 0, c.repoLimit)
	ch := make(chan fetchResponse, 1)
	for {
		req := fetchRequest{
			limit:        c.repoLimit,
			fromSequence: lastSequence + 1,
			result:       reservedEvents,
			responseChan: ch,
		}

		c.fetch(req)

		var response fetchResponse
		select {
		case res := <-ch:
			response = res
		case <-ctx.Done():
			return
		}

		if !response.existed {
			events, err := c.repo.GetEventsFromSequence(lastSequence+1, c.repoLimit)
			if err != nil {
				c.logger("repo.GetEventsFromSequence", err)
				sleepContext(ctx, c.errorTimeout)
				if ctx.Err() != nil {
					return
				}
				continue
			}
			response.result = events
		}

		if len(response.result) == 0 {
			continue
		}

		err := p.Publish(response.result)
		if err != nil {
			c.logger("p.Publish", err)
			sleepContext(ctx, c.errorTimeout)
			if ctx.Err() != nil {
				return
			}
			continue
		}

		newSequence := c.sequenceGetter(response.result[len(response.result)-1])

		err = c.repo.SaveLastSequence(p.GetID(), newSequence)
		if err != nil {
			c.logger("repo.SaveLastSequence", err)
			sleepContext(ctx, c.errorTimeout)
			if ctx.Err() != nil {
				return
			}
			continue
		}

		lastSequence = newSequence
	}
}

func (c *Core) runAsyncPublishing(ctx context.Context, p AsyncPublisher, lastSequence uint64) {
	reservedEvents := make([]Event, 0, c.repoLimit)
	ch := make(chan fetchResponse, 1)
	for {
		req := fetchRequest{
			limit:        c.repoLimit,
			fromSequence: lastSequence + 1,
			result:       reservedEvents,
			responseChan: ch,
		}

		c.fetch(req)

		var response fetchResponse
		select {
		case res := <-ch:
			response = res
		case <-ctx.Done():
			return
		}

		if !response.existed {
			events, err := c.repo.GetEventsFromSequence(lastSequence+1, c.repoLimit)
			if err != nil {
				c.logger("repo.GetEventsFromSequence", err)
				sleepContext(ctx, c.errorTimeout)
				if ctx.Err() != nil {
					return
				}
				continue
			}
			response.result = events
		}

		if len(response.result) == 0 {
			continue
		}

		err := p.PublishAsync(response.result)
		if err != nil {
			c.logger("p.Publish", err)
			return
		}

		lastSequence = c.sequenceGetter(response.result[len(response.result)-1])
	}
}

func (c *Core) runAsyncCommitting(ctx context.Context, p AsyncPublisher) {
	ch := p.GetCommitChannel()

	var commit CommittedEvent
	for {
		select {
		case first := <-ch:
			commit = first
		case <-ctx.Done():
			return
		}

	DrainLoop:
		for {
			select {
			case e := <-ch:
				commit = e
			case <-ctx.Done():
				return
			default:
				break DrainLoop
			}
		}

		if commit.Error != nil {
			c.logger("Commit channel", commit.Error)
			return
		}

		err := c.repo.SaveLastSequence(p.GetID(), commit.Sequence)
		if err != nil {
			c.logger("repo.SaveLastSequence", err)
			return
		}
	}
}

func (c *Core) runAsyncPublisherLoop(ctx context.Context, p AsyncPublisher) {
	var lastSequence uint64
	for {
		var err error
		lastSequence, err = c.repo.GetLastSequence(p.GetID())
		if err != nil {
			c.logger("repo.GetLastSequence", err)
			sleepContext(ctx, c.errorTimeout)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		break
	}

	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer wg.Done()

		c.runAsyncPublishing(ctx, p, lastSequence)
		if ctx.Err() == nil {
			cancel()
		}
	}()

	go func() {
		defer wg.Done()

		c.runAsyncCommitting(ctx, p)
		if ctx.Err() == nil {
			cancel()
		}
	}()

	wg.Wait()
}

func (c *Core) runAsyncPublisher(ctx context.Context, p AsyncPublisher) {
	for {
		c.runAsyncPublisherLoop(ctx, p)
		if ctx.Err() != nil {
			return
		}

		sleepContext(ctx, c.errorTimeout)
		if ctx.Err() != nil {
			return
		}
	}
}

func (c *Core) runLoop(ctx context.Context) {
	lastEvents, err := c.repo.GetLastEvents(c.repoLimit)
	if err != nil {
		c.logger("repo.GetLastEvents", err)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2 + len(c.publishers) + len(c.asyncPublishers))

	go func() {
		defer wg.Done()

		err := c.runDBProcessor(ctx, lastEvents)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logger("c.runDBProcessor", err)
			cancel()
		}
	}()

	go func() {
		defer wg.Done()

		c.runListener(ctx, lastEvents)
	}()

	for _, p := range c.publishers {
		publisher := p

		go func() {
			defer wg.Done()

			c.runPublisher(ctx, publisher)
		}()
	}

	for _, p := range c.asyncPublishers {
		publisher := p
		go func() {
			defer wg.Done()

			c.runAsyncPublisher(ctx, publisher)
		}()
	}

	wg.Wait()
}

// Run ...
func (c *Core) Run(ctx context.Context) {
	for {
		c.runLoop(ctx)
		if ctx.Err() != nil {
			return
		}
		sleepContext(ctx, c.errorTimeout)
		if ctx.Err() != nil {
			return
		}
	}
}

// Signal ...
func (c *Core) Signal() {
	c.signalChan <- struct{}{}
}

func (c *Core) fetch(req fetchRequest) {
	c.fetchChan <- req
}

// NewObserver ...
func (c *Core) NewObserver(fromSequence uint64) *Observer {
	return &Observer{
		sequence:       fromSequence - 1,
		core:           c,
		reservedEvents: make([]Event, 0, c.repoLimit),
		responseChan:   make(chan fetchResponse, 1),
	}
}

// GetNextEvents ...
func (o *Observer) GetNextEvents(ctx context.Context) []Event {
	for {
		req := fetchRequest{
			limit:        o.core.repoLimit,
			fromSequence: o.sequence + 1,
			result:       o.reservedEvents,
			responseChan: o.responseChan,
		}

		o.core.fetch(req)

		var response fetchResponse
		select {
		case res := <-o.responseChan:
			response = res
		case <-ctx.Done():
			return nil
		}

		if !response.existed {
			events, err := o.core.repo.GetEventsFromSequence(o.sequence+1, o.core.repoLimit)
			if err != nil {
				o.core.logger("repo.GetEventsFromSequence", err)
				sleepContext(ctx, o.core.errorTimeout)
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			response.result = events
		}

		if len(response.result) == 0 {
			return nil
		}

		o.sequence = o.core.sequenceGetter(response.result[len(response.result)-1])
		return response.result
	}
}

func sleepContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
		return
	case <-ctx.Done():
		return
	}
}
