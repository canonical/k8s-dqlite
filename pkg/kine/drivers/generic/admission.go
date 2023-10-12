package generic

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// AdmissionControlPolicy interface defines the admission policy contract.
type AdmissionControlPolicy interface {
	// Admit checks whether the query should be admitted.
	// If the query is not admitted, a non-nil error is returned with the reason why the query was denied.
	// If the query is admitted, then the error will be nil and a callback function is returned to the caller.
	// The caller must execute it after finishing the query
	Admit(ctx context.Context, txName string) (callOnFinish func(), err error)
}

const (
	statusAccepted string = "accepted"
	statusDenied   string = "denied"
)

// allowAllPolicy always admits queries.
type allowAllPolicy struct{}

// Admit always admits requests for AllowAllPolicy.
func (p *allowAllPolicy) Admit(ctx context.Context, txName string) (func(), error) {
	recordOpAdmissionControl(txName, statusAccepted)
	incCurrentOps(txName)
	return func() {
		decCurrentOps(txName)
	}, nil
}

// limitPolicy denies queries when the maximum threshold is reached.
type limitPolicy struct {
	maxConcurrentTxn int64
	semaphore        *semaphore.Weighted
}

func newLimitPolicy(maxConcurrentTxn int64) *limitPolicy {
	return &limitPolicy{
		maxConcurrentTxn: maxConcurrentTxn,
		semaphore:        semaphore.NewWeighted(maxConcurrentTxn),
	}
}

func (p *limitPolicy) Admit(ctx context.Context, txName string) (func(), error) {
	ok := p.semaphore.TryAcquire(1)
	if !ok {
		recordOpAdmissionControl(txName, statusDenied)
		return func() {}, fmt.Errorf("number of concurrent database operations reached limit (%d)", p.maxConcurrentTxn)
	}
	recordOpAdmissionControl(txName, statusAccepted)
	incCurrentOps(txName)
	return func() {
		decCurrentOps(txName)
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
