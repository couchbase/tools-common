// Package ptr provides generic utility functions for operating on/creating pointers.
package ptr

// SetIfNil sets the first pointer to the other given pointer if first pointer is nil.
//
// NOTE: This is a simple convenience function meant to replace the
//
//	if x == nil {
//	    x = y
//	}
//
// code construction which takes 3 lines whilst calling this function takes one.
func SetIfNil[V any](p **V, otherP *V) {
	if p == nil || *p != nil {
		return
	}

	*p = otherP
}
