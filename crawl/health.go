package crawl

import "errors"

var ErrNotFound = errors.New("page not found")
var ErrBlocked  = errors.New("blocked")
var ErrTimeout  = errors.New("timeout")