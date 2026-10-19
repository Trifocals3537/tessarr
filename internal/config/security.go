package config

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// ValidateDeployment catches network exposure that is syntactically valid but
// unsafe for a service intended to leave loopback. It is deliberately separate
// from Validate because Validate also drives the first-run setup state.
func (c *Config) ValidateDeployment() error {
	if _, err := ParseAllowedClientCIDRs(c.AllowedClientCIDRs); err != nil {
		return err
	}
	if IsLoopbackBindAddress(c.BindAddress) {
		return nil
	}
	if !c.UseAuth {
		return fmt.Errorf(
			"authentication is required on a non-loopback listener; use --set-auth",
		)
	}
	if !c.DisableWebDav && !c.EnableWebdavAuth {
		return fmt.Errorf(
			"WebDAV must be disabled or protected by authentication on a non-loopback listener",
		)
	}
	return nil
}

// ParseAllowedClientCIDRs compiles configured client networks once at server
// construction. Individual IP addresses are accepted as host prefixes to make
// a one-address proxy or seedbox-subnet policy easy to express.
func ParseAllowedClientCIDRs(values []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(values))
	for index, value := range values {
		raw := strings.TrimSpace(value)
		if raw == "" {
			return nil, fmt.Errorf(
				"allowed_client_cidrs entry %d is empty",
				index+1,
			)
		}

		prefix, err := parseClientPrefix(raw)
		if err != nil {
			return nil, fmt.Errorf(
				"allowed_client_cidrs entry %q is invalid: %w",
				raw,
				err,
			)
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func parseClientPrefix(raw string) (netip.Prefix, error) {
	if address, err := netip.ParseAddr(raw); err == nil {
		address = address.Unmap()
		return netip.PrefixFrom(address, address.BitLen()), nil
	}

	prefix, err := netip.ParsePrefix(raw)
	if err != nil {
		return netip.Prefix{}, err
	}
	address := prefix.Addr()
	if address.Is4In6() {
		bits := prefix.Bits() - 96
		if bits < 0 {
			return netip.Prefix{}, fmt.Errorf(
				"IPv4-mapped prefix is wider than IPv4",
			)
		}
		address = address.Unmap()
		prefix = netip.PrefixFrom(address, bits)
	}
	return prefix.Masked(), nil
}

func IsLoopbackBindAddress(bindAddress string) bool {
	host := strings.TrimSpace(bindAddress)
	if strings.EqualFold(host, "localhost") {
		return true
	}

	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	if zoneIndex := strings.LastIndexByte(host, '%'); zoneIndex >= 0 {
		host = host[:zoneIndex]
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
