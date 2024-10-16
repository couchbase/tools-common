package log

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func removeTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}

	return a
}

func TestUserData(t *testing.T) {
	var b bytes.Buffer

	l := slog.New(slog.NewTextHandler(&b, &slog.HandlerOptions{ReplaceAttr: removeTime}))

	l.Info("let's print out the user's name", UserData("name", "Ronnie O' Sullivan"))
	l.Info("and now their email", "email", UserDataValue("rocket@snooker.com"))

	require.Equal(t, `level=INFO msg="let's print out the user's name" name="<ud>Ronnie O' Sullivan</ud>"
level=INFO msg="and now their email" email=<ud>rocket@snooker.com</ud>
`, b.String())
}
