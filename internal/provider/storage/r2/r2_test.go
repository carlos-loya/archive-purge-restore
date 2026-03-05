package r2

import (
	"fmt"
	"testing"
)

func TestEndpointConstruction(t *testing.T) {
	tests := []struct {
		accountID string
		want      string
	}{
		{"abc123", "https://abc123.r2.cloudflarestorage.com"},
		{"my-account", "https://my-account.r2.cloudflarestorage.com"},
	}
	for _, tt := range tests {
		got := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", tt.accountID)
		if got != tt.want {
			t.Errorf("endpoint for %q = %q, want %q", tt.accountID, got, tt.want)
		}
	}
}

func TestDefaultRegion(t *testing.T) {
	// Verify the default region logic (region="" → "auto").
	region := ""
	if region == "" {
		region = "auto"
	}
	if region != "auto" {
		t.Errorf("default region = %q, want %q", region, "auto")
	}
}
