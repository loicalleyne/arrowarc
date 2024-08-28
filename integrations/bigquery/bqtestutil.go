package integrations

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// UniqueBQName returns a more unique name for a BigQuery resource.
func UniqueBQName(prefix string) (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate bq uuid: %w", err)
	}
	return fmt.Sprintf("%s_%s", sanitize(prefix, "_"), sanitize(u.String(), "_")), nil
}

// UniqueBucketName returns a more unique name cloud storage bucket.
func UniqueBucketName(prefix, projectID string) (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate bucket uuid: %w", err)
	}
	f := fmt.Sprintf("%s-%s-%s", sanitize(prefix, "-"), sanitize(projectID, "-"), sanitize(u.String(), "-"))
	// bucket max name length is 63 chars, so we truncate.
	if len(f) > 63 {
		f = f[:63]
	}
	// a trailing dash would make an invalid bucket name
	f = strings.TrimSuffix(f, "-")
	return f, nil
}

func sanitize(s string, allowedSeparator string) string {
	pattern := fmt.Sprintf("[^a-zA-Z0-9%s]", allowedSeparator)
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return s
	}
	return reg.ReplaceAllString(s, "")
}
