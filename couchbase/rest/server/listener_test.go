package server

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func getPortFromListener(t *testing.T, ln net.Listener) uint16 {
	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return uint16(addr.Port)
}

func TestGetListeners(t *testing.T) {
	var (
		ln4 net.Listener
		ln6 net.Listener
		err error
	)

	var port uint16

	t.Run("only IPv4", func(t *testing.T) {
		ln4, ln6, err = GetListeners(GetListenersOptions{IPv4Mode: ListenerModeMust, IPv6Mode: ListenerModeSkip, Port: 0})
		require.NoError(t, err)
		require.Nil(t, ln6)
		require.NotNil(t, ln4)

		port = getPortFromListener(t, ln4)
	})

	defer ln4.Close()

	t.Run("only IPv6", func(t *testing.T) {
		var shouldBeNil net.Listener
		shouldBeNil, ln6, err = GetListeners(GetListenersOptions{
			IPv4Mode: ListenerModeSkip,
			IPv6Mode: ListenerModeMust,
			Port:     port,
		})
		require.NoError(t, err)
		require.Nil(t, shouldBeNil)
		require.NotNil(t, ln6)
	})

	defer ln6.Close()

	t.Run("both fail", func(t *testing.T) {
		ln1, ln2, err := GetListeners(GetListenersOptions{
			IPv4Mode: ListenerModeMust,
			IPv6Mode: ListenerModeMust,
			Port:     port,
		})
		require.Error(t, err)
		require.Nil(t, ln1)
		require.Nil(t, ln2)
	})

	t.Run("one fail", func(t *testing.T) {
		ln1, ln2, err := GetListeners(GetListenersOptions{
			IPv4Mode: ListenerModeTry,
			IPv6Mode: ListenerModeMust,
			Port:     port,
		})
		require.Error(t, err)
		require.Nil(t, ln1)
		require.Nil(t, ln2)
	})

	t.Run("optional", func(t *testing.T) {
		ln1, ln2, err := GetListeners(GetListenersOptions{
			IPv4Mode: ListenerModeTry,
			IPv6Mode: ListenerModeTry,
			Port:     port,
		})
		require.NoError(t, err)
		require.Nil(t, ln1)
		require.Nil(t, ln2)
	})

	t.Run("both required", func(t *testing.T) {
		ln1, ln2, err := GetListeners(GetListenersOptions{IPv4Mode: ListenerModeMust, IPv6Mode: ListenerModeMust, Port: 0})
		require.NoError(t, err)
		require.NotNil(t, ln1)
		require.NotNil(t, ln2)

		require.NoError(t, ln1.Close())
		require.NoError(t, ln2.Close())
	})
}

func TestGetListenersNotLocalOnly(t *testing.T) {
	ln4, ln6, err := GetListeners(GetListenersOptions{IPv4Mode: ListenerModeMust, IPv6Mode: ListenerModeMust, Port: 9675})
	require.NoError(t, err)
	require.NotNil(t, ln4)
	require.Equal(t, "0.0.0.0:9675", ln4.Addr().String())
	require.NotNil(t, ln6)
	require.Equal(t, "[::]:9675", ln6.Addr().String())

	require.NoError(t, ln4.Close())
	require.NoError(t, ln6.Close())
}

func TestGetListenersLocalOnly(t *testing.T) {
	ln4, ln6, err := GetListeners(GetListenersOptions{
		IPv4Mode:  ListenerModeMust,
		IPv6Mode:  ListenerModeMust,
		Port:      9675,
		LocalOnly: true,
	})
	require.NoError(t, err)
	require.NotNil(t, ln4)
	require.Equal(t, "127.0.0.1:9675", ln4.Addr().String())
	require.NotNil(t, ln6)
	require.Equal(t, "[::1]:9675", ln6.Addr().String())

	require.NoError(t, ln4.Close())
	require.NoError(t, ln6.Close())
}
