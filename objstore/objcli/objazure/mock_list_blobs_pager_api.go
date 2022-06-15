// Code generated by mockery v2.10.4. DO NOT EDIT.

package objazure

import (
	context "context"

	azblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	mock "github.com/stretchr/testify/mock"
)

// mockListBlobsPagerAPI is an autogenerated mock type for the listBlobsPagerAPI type
type mockListBlobsPagerAPI struct {
	mock.Mock
}

// GetNextListBlobsSegment provides a mock function with given fields: ctx
func (_m *mockListBlobsPagerAPI) GetNextListBlobsSegment(ctx context.Context) ([]*azblob.BlobPrefix, []*azblob.BlobItemInternal, error) {
	ret := _m.Called(ctx)

	var r0 []*azblob.BlobPrefix
	if rf, ok := ret.Get(0).(func(context.Context) []*azblob.BlobPrefix); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*azblob.BlobPrefix)
		}
	}

	var r1 []*azblob.BlobItemInternal
	if rf, ok := ret.Get(1).(func(context.Context) []*azblob.BlobItemInternal); ok {
		r1 = rf(ctx)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*azblob.BlobItemInternal)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context) error); ok {
		r2 = rf(ctx)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}
