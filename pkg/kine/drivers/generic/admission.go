package generic

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"
)

// AdmissionControlPolicy interface defines the admission policy contract.
type AdmissionControlPolicy interface {
	// Admit checks whether the query should be admitted.
	// If the query is not admitted, a non-nil error is returned with the reason why the query was denied.
	// If the query is admitted, then the error will be nil and a callback function is returned to the caller.
	// The caller must execute it after finishing the query
	Admit(context.Context) (callOnFinish func(), err error)
}

// allowAllPolicy always admits queries.
type allowAllPolicy struct{}

// Admit always admits requests for AllowAllPolicy.
func (p *allowAllPolicy) Admit(context.Context) (func(), error) {
	return func() {}, nil
}

// limitPolicy denies queries when the maximum threshold is reached.
type limitPolicy struct {
	MaxRunningTxn int64
	semaphore     *semaphore.Weighted
}

func newLimitPolicy(maxRunningTxn int64) *limitPolicy {
	return &limitPolicy{
		MaxRunningTxn: maxRunningTxn,
		semaphore:     semaphore.NewWeighted(maxRunningTxn),
	}
}

func (p *limitPolicy) admit(ctx context.Context) (func(), error) {
	ok := p.semaphore.TryAcquire(1)
	if !ok {
		return func() {}, fmt.Errorf("current Txns reached limit (%d)", p.MaxRunningTxn)
	}
	return func() {
		p.semaphore.Release(1)
	}, nil
}
