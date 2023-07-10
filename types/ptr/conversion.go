package ptr

// To returns a pointer to a copy of the provided object.
//
// NOTE: This is required for getting a pointer to a value that is defined as a Go constant (not the constant itself,
// passing it to a function will create a variable copy with the same value). For variables the pointer can be acquired
// directly using the & operator so there is no point in using this function in that case.
func To[V any](v V) *V {
	return &v
}
