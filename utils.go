package zes_search

import (
	"context"
	"errors"
	"reflect"
	"time"
)

var ErrNoDocuments = errors.New("没有匹配的结果")

// 构建用于超时的上下文
func makeTimeoutCtx(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(context.Background())
	}

	return context.WithTimeout(context.Background(), timeout)
}

// 构建用于超时的上下文
func makeTimeoutCtxWithBaseCtx(baseCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(baseCtx)
	}

	return context.WithTimeout(baseCtx, timeout)
}

// 检查输出参数, 它必须是一个已初始化的指针, 返回(指向的value, 指向的type, 是否非切片)
func checkOutParam(a interface{}) (aValue reflect.Value, aType reflect.Type, one bool) {
	aType = reflect.TypeOf(a)
	if aType.Kind() != reflect.Ptr {
		panic(errors.New("a必须是一个指针"))
	}

	aType = aType.Elem()
	aValue = reflect.ValueOf(a).Elem()

	if aType.Kind() == reflect.Invalid {
		panic(errors.New("a是无效的, 它可能未初始化"))
	}

	one = aType.Kind() != reflect.Slice
	return
}

// 检查切片类型, 返回(item类型,item是否带指针)
func checkSliceType(sliceType reflect.Type) (itemType reflect.Type, itemIsPtr bool) {
	itemType = sliceType.Elem()
	itemIsPtr = itemType.Kind() == reflect.Ptr
	if itemIsPtr {
		itemType = itemType.Elem()
	}
	checkItemType(itemType)
	return
}

// 检查 item 类型, 它必须是 interface{}, map 或 struct
func checkItemType(itemType reflect.Type) {
	switch itemType.Kind() {
	case reflect.Interface, reflect.Map, reflect.Struct:
		return
	}
	panic(errors.New("item必须是 interface{}, map 或 struct, 它可以选择带指针"))
}
