package arr

import (
	"crypto/subtle"
	"sort"
	"strings"
)

const maxCredentialCompareBytes = 4096

// MatchCredentials returns a configured Arr whose host and token match the
// supplied credentials. Authentication never probes a caller-supplied URL;
// only hosts already admitted into the runtime configuration can match.
func (s *Storage) MatchCredentials(category, host, token string) *Arr {
	if s == nil || strings.TrimSpace(host) == "" || strings.TrimSpace(token) == "" {
		return nil
	}
	host = strings.TrimSpace(host)
	token = strings.TrimSpace(token)

	if category != "" {
		if candidate := s.Get(category); arrCredentialsEqual(candidate, host, token) {
			return candidate
		}
	}

	candidates := s.GetAll()
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Name < candidates[j].Name
	})
	for _, candidate := range candidates {
		if arrCredentialsEqual(candidate, host, token) {
			return candidate
		}
	}
	return nil
}

func arrCredentialsEqual(candidate *Arr, host, token string) bool {
	if candidate == nil {
		return false
	}
	return constantTimeStringEqual(strings.TrimSpace(candidate.Host), host) &&
		constantTimeStringEqual(strings.TrimSpace(candidate.Token), token)
}

func constantTimeStringEqual(left, right string) bool {
	if len(left) > maxCredentialCompareBytes || len(right) > maxCredentialCompareBytes {
		return false
	}

	var leftPadded, rightPadded [maxCredentialCompareBytes]byte
	copy(leftPadded[:], left)
	copy(rightPadded[:], right)

	contentsEqual := subtle.ConstantTimeCompare(leftPadded[:], rightPadded[:])
	lengthsEqual := subtle.ConstantTimeEq(int32(len(left)), int32(len(right)))
	return contentsEqual&lengthsEqual == 1
}
