package generic

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// AdmissionControlPolicy interface defines the admission policy contract.
type AdmissionControlPolicy interface {
	// admit checks whether the query should be admitted.
	// If the query is not admitted, a non-nil error is returned with the reason why the query was denied.
	// If the query is admitted, then the error will be nil and a callback function is returned to the caller.
	// The caller must execute it after finishing the query
	admit(context.Context) (callOnFinish func(), err error)
}

// allowAllPolicy always admits queries.
type allowAllPolicy struct{}

// admit always admits requests for AllowAllPolicy.
func (p *allowAllPolicy) admit(context.Context) (func(), error) {
	return func() {}, nil
}

// limitPolicy denies queries when the maximum threshold is reached.
type limitPolicy struct {
	MaxConcurrentTxn int64
	semaphore        *semaphore.Weighted
}

func newLimitPolicy(maxConcurrentTxn int64) *limitPolicy {
	return &limitPolicy{
		MaxConcurrentTxn: maxConcurrentTxn,
		semaphore:        semaphore.NewWeighted(maxConcurrentTxn),
	}
}

func (p *limitPolicy) admit(ctx context.Context) (func(), error) {
	ok := p.semaphore.TryAcquire(1)
	if !ok {
		return func() {}, fmt.Errorf("current Txns reached limit (%d)", p.MaxConcurrentTxn)
	}
	return func() {
		p.semaphore.Release(1)
	}, nil
}

func NewAdmissionControlPolicy(policyName string, limitMaxConcurrentTxn int64) AdmissionControlPolicy {
	switch policyName {
	case "limit":
		return newLimitPolicy(limitMaxConcurrentTxn)
	case "allow-all":
		return &allowAllPolicy{}

	default:
		logrus.Warnf("unknown admission control policy %q - fallback to 'allow-all'", policyName)
		return &allowAllPolicy{}
	}
}
