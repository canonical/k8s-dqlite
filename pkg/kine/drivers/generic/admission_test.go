package generic

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
)

func TestAllowAllPolicy_Admit(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	policy := &allowAllPolicy{}

	callOnFinish, err := policy.Admit(context.Background())

	g.Expect(err).To(gomega.BeNil())
	g.Expect(callOnFinish).ToNot(gomega.BeNil())
}

func TestLimitPolicy_Admit(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	policy := newLimitPolicy(2)

	// First two should succeed
	done1, err := policy.admit(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(done1).ToNot(gomega.BeNil())
	done2, err := policy.admit(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(done2).ToNot(gomega.BeNil())

	// Third should be denied
	done3, err := policy.admit(context.Background())
	g.Expect(err).To(gomega.HaveOccurred())
	// should still return a valid function as callers otherwise might segfault if they not check for nil.
	g.Expect(done3).ToNot(gomega.BeNil())

	// Complete a call - now the next query should be admitted again.
	done1()
	_, err = policy.admit(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(done3).ToNot(gomega.BeNil())
}
