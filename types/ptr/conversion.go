package ptr

// To returns a pointer to a copy of the provided object.
//
// NOTE: This is required for getting a pointer to a value that is defined as a Go constant (not the constant itself,
// passing it to a function will create a variable copy with the same value). For variables the pointer can be acquired
// directly using the & operator so there is no point in using this function in that case.
func To[V any](v V) *V {
	return &v
}

// From dereferences the given pointer or returns the default value if <nil>, this mimics lots of the utility functions
// in the AWS SDK which do the same thing but were written before generics.
func From[V any](v *V) V {
	if v != nil {
		return *v
	}

	return *new(V)
}
