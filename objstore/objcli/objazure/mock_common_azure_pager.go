// Code generated by mockery v2.10.4. DO NOT EDIT.

package objazure

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// mockCommonAzurePager is an autogenerated mock type for the commonAzurePager type
type mockCommonAzurePager struct {
	mock.Mock
}

// Err provides a mock function with given fields:
func (_m *mockCommonAzurePager) Err() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NextPage provides a mock function with given fields: ctx
func (_m *mockCommonAzurePager) NextPage(ctx context.Context) bool {
	ret := _m.Called(ctx)

	var r0 bool
	if rf, ok := ret.Get(0).(func(context.Context) bool); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}