// Code generated by mockery v2.53.3. DO NOT EDIT.

package objgcp

import (
	context "context"

	storage "cloud.google.com/go/storage"
	mock "github.com/stretchr/testify/mock"
)

// mockObjectAPI is an autogenerated mock type for the objectAPI type
type mockObjectAPI struct {
	mock.Mock
}

// Attrs provides a mock function with given fields: ctx
func (_m *mockObjectAPI) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Attrs")
	}

	var r0 *storage.ObjectAttrs
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*storage.ObjectAttrs, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *storage.ObjectAttrs); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*storage.ObjectAttrs)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ComposerFrom provides a mock function with given fields: srcs
func (_m *mockObjectAPI) ComposerFrom(srcs ...objectAPI) composeAPI {
	_va := make([]interface{}, len(srcs))
	for _i := range srcs {
		_va[_i] = srcs[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ComposerFrom")
	}

	var r0 composeAPI
	if rf, ok := ret.Get(0).(func(...objectAPI) composeAPI); ok {
		r0 = rf(srcs...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(composeAPI)
		}
	}

	return r0
}

// CopierFrom provides a mock function with given fields: src
func (_m *mockObjectAPI) CopierFrom(src objectAPI) copierAPI {
	ret := _m.Called(src)

	if len(ret) == 0 {
		panic("no return value specified for CopierFrom")
	}

	var r0 copierAPI
	if rf, ok := ret.Get(0).(func(objectAPI) copierAPI); ok {
		r0 = rf(src)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(copierAPI)
		}
	}

	return r0
}

// Delete provides a mock function with given fields: ctx
func (_m *mockObjectAPI) Delete(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Delete")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Generation provides a mock function with given fields: gen
func (_m *mockObjectAPI) Generation(gen int64) objectAPI {
	ret := _m.Called(gen)

	if len(ret) == 0 {
		panic("no return value specified for Generation")
	}

	var r0 objectAPI
	if rf, ok := ret.Get(0).(func(int64) objectAPI); ok {
		r0 = rf(gen)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(objectAPI)
		}
	}

	return r0
}

// If provides a mock function with given fields: conds
func (_m *mockObjectAPI) If(conds storage.Conditions) objectAPI {
	ret := _m.Called(conds)

	if len(ret) == 0 {
		panic("no return value specified for If")
	}

	var r0 objectAPI
	if rf, ok := ret.Get(0).(func(storage.Conditions) objectAPI); ok {
		r0 = rf(conds)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(objectAPI)
		}
	}

	return r0
}

// NewRangeReader provides a mock function with given fields: ctx, offset, length
func (_m *mockObjectAPI) NewRangeReader(ctx context.Context, offset, length int64) (readerAPI, error) {
	ret := _m.Called(ctx, offset, length)

	if len(ret) == 0 {
		panic("no return value specified for NewRangeReader")
	}

	var r0 readerAPI
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (readerAPI, error)); ok {
		return rf(ctx, offset, length)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) readerAPI); ok {
		r0 = rf(ctx, offset, length)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(readerAPI)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, offset, length)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewWriter provides a mock function with given fields: ctx
func (_m *mockObjectAPI) NewWriter(ctx context.Context) writerAPI {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for NewWriter")
	}

	var r0 writerAPI
	if rf, ok := ret.Get(0).(func(context.Context) writerAPI); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(writerAPI)
		}
	}

	return r0
}

// Retryer provides a mock function with given fields: opts
func (_m *mockObjectAPI) Retryer(opts ...storage.RetryOption) objectAPI {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Retryer")
	}

	var r0 objectAPI
	if rf, ok := ret.Get(0).(func(...storage.RetryOption) objectAPI); ok {
		r0 = rf(opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(objectAPI)
		}
	}

	return r0
}

// Update provides a mock function with given fields: ctx, uattrs
func (_m *mockObjectAPI) Update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	ret := _m.Called(ctx, uattrs)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 *storage.ObjectAttrs
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error)); ok {
		return rf(ctx, uattrs)
	}
	if rf, ok := ret.Get(0).(func(context.Context, storage.ObjectAttrsToUpdate) *storage.ObjectAttrs); ok {
		r0 = rf(ctx, uattrs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*storage.ObjectAttrs)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, storage.ObjectAttrsToUpdate) error); ok {
		r1 = rf(ctx, uattrs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockObjectAPI creates a new instance of mockObjectAPI. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockObjectAPI(t interface {
	mock.TestingT
	Cleanup(func())
},
) *mockObjectAPI {
	mock := &mockObjectAPI{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
