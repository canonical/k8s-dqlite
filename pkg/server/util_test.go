package server

import (
	"math"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestBusyTimeoutOption(t *testing.T) {
	t.Run("ZeroKeepsFailFastDefault", func(t *testing.T) {
		g := NewWithT(t)

		option, err := busyTimeoutOption(0)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(option).To(BeNil())
	})

	t.Run("SubMillisecondRoundsToZero", func(t *testing.T) {
		g := NewWithT(t)

		option, err := busyTimeoutOption(999 * time.Microsecond)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(option).To(BeNil())
	})

	t.Run("PositiveReturnsOption", func(t *testing.T) {
		g := NewWithT(t)

		for _, timeout := range []time.Duration{time.Millisecond, time.Second, time.Duration(math.MaxUint32) * time.Millisecond} {
			option, err := busyTimeoutOption(timeout)
			g.Expect(err).ToNot(HaveOccurred(), "timeout %v", timeout)
			g.Expect(option).ToNot(BeNil(), "timeout %v", timeout)
		}
	})

	t.Run("NegativeFails", func(t *testing.T) {
		g := NewWithT(t)

		option, err := busyTimeoutOption(-time.Millisecond)
		g.Expect(err).To(MatchError(ContainSubstring("must not be negative")))
		g.Expect(option).To(BeNil())
	})

	t.Run("BeyondUint32MillisecondsFails", func(t *testing.T) {
		g := NewWithT(t)

		option, err := busyTimeoutOption(time.Duration(math.MaxUint32+1) * time.Millisecond)
		g.Expect(err).To(MatchError(ContainSubstring("must not exceed")))
		g.Expect(option).To(BeNil())
	})
}
