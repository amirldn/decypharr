package link

import "testing"

func TestRejectedLinkRefetchPreservesOtherErrorPolicies(t *testing.T) {
	for _, tc := range []struct {
		code string
		want ErrorCategory
	}{
		{"400", CategoryRefetchable},
		{"401", CategoryPermanent},
		{"404", CategoryPermanent},
		{"429", CategoryRetryable},
		{"unknown", CategoryPermanent},
	} {
		t.Run(tc.code, func(t *testing.T) {
			if got := ErrorCodeToLinkError(tc.code); got.Category != tc.want {
				t.Fatalf("category = %v, want %v", got.Category, tc.want)
			}
		})
	}
}
