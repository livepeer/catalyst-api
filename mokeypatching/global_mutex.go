package mokeypatching

import "sync"

// This should be global and unique mutex for all tests to use for all vars they need to configure on test start
// If we used multiple mutexes then deadlock is possible, with single mutex test run in sequence
var MonkeypatchingMutex sync.Mutex
