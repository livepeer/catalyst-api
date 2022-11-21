package metrics

type contextKey string

func (c contextKey) String() string {
	return "clientsContextKey" + string(c)
}

var RetriesKey = contextKey("CatalystAPIRetries")
