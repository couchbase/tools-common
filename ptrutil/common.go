package ptrutil

// ToPtr returns a pointer to a copy of the provided object.
//
// NOTE: This is required for getting a pointer to a value that is defined as a Go constant (not the constant itself,
// passing it to a function will create a variable copy with the same value). For variables the pointer can be acquired
// directly using the & operator so there is no point in using this function in that case.
func ToPtr[V any](v V) *V {
	return &v
}

// SetPtrIfNil sets the first pointer to the other given pointer if first pointer is nil.
//
// NOTE: This is a simple convenience function meant to replace the
//
//	if x == nil {
//	    x = y
//	}
//
// code construction which takes 3 lines whilst calling this function takes one.
func SetPtrIfNil[V any](p **V, otherP *V) {
	if p == nil || *p != nil {
		return
	}

	*p = otherP
}
