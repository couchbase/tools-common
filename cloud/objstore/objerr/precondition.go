package objerr

import "fmt"

type PreconditionFailedError struct {
	Key string
}

func (p *PreconditionFailedError) Error() string {
	return fmt.Sprintf("update to %q failed due to precondition", p.Key)
}
