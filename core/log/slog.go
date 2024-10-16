package log

import (
	"fmt"
	"log/slog"
)

// UserDataValue is a string that should be treated as user data, and therefore tagged as such in the logs.
type UserDataValue string

func (u UserDataValue) LogValue() slog.Value {
	return slog.StringValue(fmt.Sprintf("<ud>%s</ud>", u))
}

// UserData returns an Attr for a string value that should be treated as user data.
func UserData(key, value string) slog.Attr {
	return slog.Attr{Key: key, Value: UserDataValue(value).LogValue()}
}
