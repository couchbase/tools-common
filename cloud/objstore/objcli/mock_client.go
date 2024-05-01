// Code generated by mockery v2.43.0. DO NOT EDIT.

package objcli

import (
	context "context"

	objval "github.com/couchbase/tools-common/cloud/v5/objstore/objval"
	mock "github.com/stretchr/testify/mock"
)

// MockClient is an autogenerated mock type for the Client type
type MockClient struct {
	mock.Mock
}

// AbortMultipartUpload provides a mock function with given fields: ctx, opts
func (_m *MockClient) AbortMultipartUpload(ctx context.Context, opts AbortMultipartUploadOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for AbortMultipartUpload")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, AbortMultipartUploadOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AppendToObject provides a mock function with given fields: ctx, opts
func (_m *MockClient) AppendToObject(ctx context.Context, opts AppendToObjectOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for AppendToObject")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, AppendToObjectOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *MockClient) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CompleteMultipartUpload provides a mock function with given fields: ctx, opts
func (_m *MockClient) CompleteMultipartUpload(ctx context.Context, opts CompleteMultipartUploadOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for CompleteMultipartUpload")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, CompleteMultipartUploadOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CopyObject provides a mock function with given fields: ctx, opts
func (_m *MockClient) CopyObject(ctx context.Context, opts CopyObjectOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for CopyObject")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, CopyObjectOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateMultipartUpload provides a mock function with given fields: ctx, opts
func (_m *MockClient) CreateMultipartUpload(ctx context.Context, opts CreateMultipartUploadOptions) (string, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for CreateMultipartUpload")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, CreateMultipartUploadOptions) (string, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, CreateMultipartUploadOptions) string); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, CreateMultipartUploadOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeleteDirectory provides a mock function with given fields: ctx, opts
func (_m *MockClient) DeleteDirectory(ctx context.Context, opts DeleteDirectoryOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for DeleteDirectory")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, DeleteDirectoryOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteObjects provides a mock function with given fields: ctx, opts
func (_m *MockClient) DeleteObjects(ctx context.Context, opts DeleteObjectsOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for DeleteObjects")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, DeleteObjectsOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetObject provides a mock function with given fields: ctx, opts
func (_m *MockClient) GetObject(ctx context.Context, opts GetObjectOptions) (*objval.Object, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for GetObject")
	}

	var r0 *objval.Object
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, GetObjectOptions) (*objval.Object, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, GetObjectOptions) *objval.Object); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*objval.Object)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, GetObjectOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetObjectAttrs provides a mock function with given fields: ctx, opts
func (_m *MockClient) GetObjectAttrs(ctx context.Context, opts GetObjectAttrsOptions) (*objval.ObjectAttrs, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for GetObjectAttrs")
	}

	var r0 *objval.ObjectAttrs
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, GetObjectAttrsOptions) (*objval.ObjectAttrs, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, GetObjectAttrsOptions) *objval.ObjectAttrs); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*objval.ObjectAttrs)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, GetObjectAttrsOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// IterateObjects provides a mock function with given fields: ctx, opts
func (_m *MockClient) IterateObjects(ctx context.Context, opts IterateObjectsOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for IterateObjects")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, IterateObjectsOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ListParts provides a mock function with given fields: ctx, opts
func (_m *MockClient) ListParts(ctx context.Context, opts ListPartsOptions) ([]objval.Part, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for ListParts")
	}

	var r0 []objval.Part
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, ListPartsOptions) ([]objval.Part, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, ListPartsOptions) []objval.Part); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]objval.Part)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, ListPartsOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Provider provides a mock function with given fields:
func (_m *MockClient) Provider() objval.Provider {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Provider")
	}

	var r0 objval.Provider
	if rf, ok := ret.Get(0).(func() objval.Provider); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(objval.Provider)
	}

	return r0
}

// PutObject provides a mock function with given fields: ctx, opts
func (_m *MockClient) PutObject(ctx context.Context, opts PutObjectOptions) error {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for PutObject")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, PutObjectOptions) error); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UploadPart provides a mock function with given fields: ctx, opts
func (_m *MockClient) UploadPart(ctx context.Context, opts UploadPartOptions) (objval.Part, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for UploadPart")
	}

	var r0 objval.Part
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, UploadPartOptions) (objval.Part, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, UploadPartOptions) objval.Part); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Get(0).(objval.Part)
	}

	if rf, ok := ret.Get(1).(func(context.Context, UploadPartOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UploadPartCopy provides a mock function with given fields: ctx, opts
func (_m *MockClient) UploadPartCopy(ctx context.Context, opts UploadPartCopyOptions) (objval.Part, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for UploadPartCopy")
	}

	var r0 objval.Part
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, UploadPartCopyOptions) (objval.Part, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, UploadPartCopyOptions) objval.Part); ok {
		r0 = rf(ctx, opts)
	} else {
		r0 = ret.Get(0).(objval.Part)
	}

	if rf, ok := ret.Get(1).(func(context.Context, UploadPartCopyOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewMockClient creates a new instance of MockClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockClient {
	mock := &MockClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
