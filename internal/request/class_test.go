package request

import (
	"context"
	"testing"
)

func TestPlaybackUnlessSet(t *testing.T) {
	if got := ClassOf(PlaybackUnlessSet(context.Background())); got != ClassPlayback {
		t.Fatalf("unset context: got %s, want playback", got)
	}
	bg := WithClass(context.Background(), ClassBackground)
	if !HasClass(bg) {
		t.Fatal("explicit background not detected")
	}
	if got := ClassOf(PlaybackUnlessSet(bg)); got != ClassBackground {
		t.Fatalf("explicit background overridden to %s", got)
	}
	if HasClass(context.Background()) || HasClass(nil) {
		t.Fatal("HasClass true for an untagged context")
	}
}
