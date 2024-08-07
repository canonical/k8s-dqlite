package generic

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
)

func TestAllowAllPolicy_Admit(t *testing.T) {
	g := NewWithT(t)
	policy := &allowAllPolicy{}

	callOnFinish, err := policy.Admit(context.Background(), "get_size_sql")

	g.Expect(err).To(BeNil())
	g.Expect(callOnFinish).ToNot(BeNil())
}

func TestLimitPolicy_Admit(t *testing.T) {
	g := NewWithT(t)
	policy := newLimitPolicy(false, 2)

	// First two should succeed
	done1, err := policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done1).ToNot(BeNil())
	done2, err := policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done2).ToNot(BeNil())

	// Third should be denied
	done3, err := policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).To(HaveOccurred())
	// should still return a valid function as callers otherwise might segfault if they not check for nil.
	g.Expect(done3).ToNot(BeNil())

	// Complete a call - now the next query should be admitted again.
	done1()
	_, err = policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done3).ToNot(BeNil())
}

func TestLimitPolicy_Admit_OnlyCheckWriteQueries(t *testing.T) {
	g := NewWithT(t)
	policy := newLimitPolicy(true, 2)

	// Read queries should always succeed
	_, err := policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())
	_, err = policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())
	_, err = policy.Admit(context.Background(), "get_size_sql")
	g.Expect(err).ToNot(HaveOccurred())

	// write queries should be evaluated, thus fail after second call
	done1, err := policy.Admit(context.Background(), "insert_sql")
	g.Expect(err).ToNot(HaveOccurred())
	_, err = policy.Admit(context.Background(), "delete_sql")
	g.Expect(err).ToNot(HaveOccurred())
	_, err = policy.Admit(context.Background(), "fill_sql")
	g.Expect(err).To(HaveOccurred())

	// Another write query should be possible after one is done
	done1()
	_, err = policy.Admit(context.Background(), "fill_sql")
	g.Expect(err).ToNot(HaveOccurred())
}
