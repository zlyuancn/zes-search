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

// 检查输出参数, 它必须是一个已初始化的指针, 返回(指向的value, 指向的type, 是否非切片)
func checkOutParam(a interface{}) (a_value reflect.Value, a_type reflect.Type, one bool) {
	a_type = reflect.TypeOf(a)
	if a_type.Kind() != reflect.Ptr {
		panic(errors.New("a必须是一个指针"))
	}

	a_type = a_type.Elem()
	a_value = reflect.ValueOf(a).Elem()

	if a_type.Kind() == reflect.Invalid {
		panic(errors.New("a是无效的, 它可能未初始化"))
	}

	one = a_type.Kind() != reflect.Slice
	return
}

// 检查切片类型, 返回(item类型,item是否带指针)
func checkSliceType(slice_type reflect.Type) (item_type reflect.Type, item_is_ptr bool) {
	item_type = slice_type.Elem()
	item_is_ptr = item_type.Kind() == reflect.Ptr
	if item_is_ptr {
		item_type = item_type.Elem()
	}
	checkItemType(item_type)
	return
}

// 检查 item 类型, 它必须是 interface{}, map 或 struct
func checkItemType(item_type reflect.Type) {
	item_kind := item_type.Kind()
	if item_kind != reflect.Interface && item_kind != reflect.Map && item_kind != reflect.Struct {
		panic(errors.New("item必须是 interface{}, map 或 struct, 它可以选择带指针"))
	}
}
