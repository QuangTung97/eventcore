package eventcore

import "time"

// Option ...
type Option func(opts *coreOpts)

type coreOpts struct {
	repoLimit  uint64
	fetchLimit uint64

	publishers   []Publisher
	errorTimeout time.Duration
}

var defaultCoreOpts = &coreOpts{
	repoLimit:    1000,
	fetchLimit:   100,
	errorTimeout: 1 * time.Minute,
}

// AddPublisher ...
func AddPublisher(p Publisher) Option {
	return func(opts *coreOpts) {
		opts.publishers = append(opts.publishers, p)
	}
}

// WithRepositoryLimit ...
func WithRepositoryLimit(limit uint64) Option {
	return func(opts *coreOpts) {
		opts.repoLimit = limit
	}
}

// WithErrorTimeout ...
func WithErrorTimeout(d time.Duration) Option {
	return func(opts *coreOpts) {
		opts.errorTimeout = d
	}
}

func applyOptions(opts *coreOpts, options ...Option) {
	for _, o := range options {
		o(opts)
	}
}
