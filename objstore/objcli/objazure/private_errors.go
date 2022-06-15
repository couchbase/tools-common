package objazure

import "errors"

// errPagerNoMorePages is returned when an Azure pager has finished iterating through all available blob pages. This
// error does not represent an actual error that can be thrown during execution but instead used to exit the pager page
// fetching loop.
var errPagerNoMorePages = errors.New("no more pages")
