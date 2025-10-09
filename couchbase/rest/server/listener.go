package server

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
)

type ListenerMode int

const (
	ListenerModeSkip ListenerMode = iota
	ListenerModeTry
	ListenerModeMust
)

type GetListenersOptions struct {
	IPv4Mode, IPv6Mode ListenerMode
	Port               uint16
	LocalOnly          bool
	LogPrefix          string
}

// GetListeners returns an TCP4 and/or TCP6 net listener for the given address.
func GetListeners(opts GetListenersOptions) (net.Listener, net.Listener, error) {
	if opts.LogPrefix != "" {
		opts.LogPrefix += " "
	}

	ln4, err := getListener(opts.IPv4Mode, "tcp4", opts.Port, opts.LocalOnly, opts.LogPrefix)
	if err != nil {
		return nil, nil, fmt.Errorf("could not start IPv4 listener: %w", err)
	}

	ln6, err := getListener(opts.IPv6Mode, "tcp6", opts.Port, opts.LocalOnly, opts.LogPrefix)
	if err == nil {
		return ln4, ln6, nil
	}

	if ln4 != nil {
		if err := ln4.Close(); err != nil {
			slog.Error(opts.LogPrefix+"Could not close ipv4 listener", "err", err)
		}
	}

	return nil, nil, fmt.Errorf("could not start IPv6 listener: %w", err)
}

func getListener(mode ListenerMode, family string, port uint16, localOnly bool, prefix string) (net.Listener, error) {
	if mode == ListenerModeSkip {
		return nil, nil
	}

	address := listenAddress(family, strconv.FormatUint(uint64(port), 10), localOnly)

	ln, err := net.Listen(family, address)
	if err == nil {
		return ln, nil
	}

	slog.Warn(prefix+"Could not start listener", "family", family, "address", address, "err", err)

	if mode == ListenerModeMust {
		return nil, fmt.Errorf("could not start listener: %w", err)
	}

	return nil, nil
}

func listenAddress(family, port string, localOnly bool) string {
	if !localOnly {
		return net.JoinHostPort("", port)
	}

	localhost := "localhost"
	if family == "tcp6" {
		localhost = "::1"
	}

	return net.JoinHostPort(localhost, port)
}
