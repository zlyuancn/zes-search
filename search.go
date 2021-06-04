/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/23
   Description :
-------------------------------------------------
*/

package zes_search

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic/v7"
)

// 解析innerHits的标签名
const (
	InnerHitsTagName       = "inner_hits"
	InnerHitsSecondTagName = "json" // 第二优先级
)

var ErrNoDocuments = errors.New("没有匹配的结果")

func makeTimeoutCtx(timeout ...time.Duration) (context.Context, context.CancelFunc) {
	if len(timeout) == 0 || timeout[0] <= 0 {
		return context.WithCancel(context.Background())
	}

	return context.WithTimeout(context.Background(), timeout[0])
}

// 搜索, 将结果写入a, timeout为0时不设置超时
func Search(ss *elastic.SearchService, a interface{}, timeout ...time.Duration) (total int, err error) {
	ctx, cancel := makeTimeoutCtx(timeout...)
	defer cancel()
	return SearchWithCtx(ctx, ss, a)
}

// 搜索, 功能同Search
func SearchWithCtx(ctx context.Context, ss *elastic.SearchService, a interface{}) (total int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 检查输出参数, 它必须是一个已初始化的指针, 返回(指向的value, 指向的type, 是否非切片)
	a_value, a_type, is_search_one := checkOutParam(a)
	if is_search_one { // 如果不是切片, 表示只要一条数据
		ss = ss.Size(1)
	}

	// 执行
	resp, err := ss.Do(ctx)
	if err != nil {
		return 0, err
	}

	// 获取总数
	total = int(resp.TotalHits())
	if total == 0 || len(resp.Hits.Hits) == 0 {
		if is_search_one { // 如果只要一条数据, 但是无数据则返回错误
			return total, ErrNoDocuments
		}
		return total, nil
	}

	// 将数据写入a
	if err = parseHits(resp.Hits.Hits, a_value, a_type); err != nil {
		return total, err
	}
	return total, nil
}

// 搜索ids, 返回匹配结果的id列表, timeout为0时不设置超时
func SearchIds(ss *elastic.SearchService, timeout ...time.Duration) ([]string, int, error) {
	ctx, cancel := makeTimeoutCtx(timeout...)
	defer cancel()
	return SearchIdsWithCtx(ctx, ss)
}

// 搜索ids, 功能同SearchIds
func SearchIdsWithCtx(ctx context.Context, ss *elastic.SearchService) ([]string, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 获取id不需要 _source
	ss.FetchSourceContext(elastic.NewFetchSourceContext(false))

	// 执行
	resp, err := ss.Do(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("在搜索时出现错误: %s", err.Error())
	}

	// 获取总数
	total := int(resp.TotalHits())
	if total == 0 || resp.Hits == nil || len(resp.Hits.Hits) == 0 {
		return nil, total, nil
	}

	// 写入id
	out := make([]string, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		out[i] = hit.Id
	}
	return out, total, nil
}

// 解析 hits 并写入值
func parseHits(hits []*elastic.SearchHit, a_value reflect.Value, a_type reflect.Type) (err error) {
	if len(hits) == 0 {
		return nil
	}

	// 如果非切片, 表示只要一条数据
	if a_type.Kind() != reflect.Slice {
		return parseOneHit(hits[0], a_value, a_type)
	}

	// 检查切片类型, 返回(item类型,item是否带指针)
	item_type, item_is_ptr := checkSliceType(a_type)

	// 遍历hits并写入对象中
	items := make([]reflect.Value, len(hits))
	for i, hit := range hits {
		child := reflect.New(item_type)
		child_value := child.Elem()

		// 将 hit 的 _source 解析到 child
		if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
			return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
		}

		// 附加, 解析所有 innerHits 到 child_value
		if err = parseAllInnerHits(hit, child_value, item_type); err != nil {
			return err
		}

		// 如果item带指针直接使用child
		if item_is_ptr {
			items[i] = child
			continue
		}

		items[i] = child_value
	}

	values := reflect.Append(a_value, items...)
	a_value.Set(values)
	return nil
}

// 解析一个 hit 并写入值
func parseOneHit(hit *elastic.SearchHit, item_value reflect.Value, item_type reflect.Type) (err error) {
	checkItemType(item_type)

	child := reflect.New(item_type)
	child_value := child.Elem()

	// 将 hit 的 _source 解析到 child
	if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
		return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
	}

	// 附加, 解析所有 innerHits 到 child_value
	if err = parseAllInnerHits(hit, child_value, item_type); err != nil {
		return err
	}

	item_value.Set(child_value)
	return nil
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

// 解析所有 innerHits 到 item
func parseAllInnerHits(hit *elastic.SearchHit, item_value reflect.Value, item_type reflect.Type) (err error) {
	if len(hit.InnerHits) == 0 {
		return nil
	}

	for inner_key, inner_hits := range hit.InnerHits {
		if err = parseInnerHit(inner_key, inner_hits, item_value, item_type); err != nil {
			return err
		}
	}
	return nil
}

// 解析一个 innerHits 到 item
func parseInnerHit(inner_key string, innerHits *elastic.SearchHitInnerHits, item_value reflect.Value, item_type reflect.Type) (err error) {
	if innerHits == nil || innerHits.Hits == nil || len(innerHits.Hits.Hits) == 0 {
		return nil
	}

	// interface 解包
	is_interface := item_type.Kind() == reflect.Interface
	if is_interface {
		item_value = item_value.Elem()
		item_type = item_value.Type()
	}
	checkItemType(item_type)

	// 开始解析
	hits := innerHits.Hits.Hits
	switch kind := item_type.Kind(); kind {
	case reflect.Map: // 如果是一个map, 那么用户无法指定inner_hits类型
		data := []map[string]interface{}{}
		child_value := reflect.ValueOf(&data).Elem() // 获取指针的反射值
		child_type := reflect.TypeOf(&data).Elem()   // 获取指针的的反射类型
		err = parseHits(hits, child_value, child_type)
		if err != nil {
			return err
		}
		item_value.SetMapIndex(reflect.ValueOf(inner_key), child_value)
	case reflect.Struct:
		// 获取结构体的指定字段
		field_value, field_type, ok := searchStructFieldWithInnerHit(item_value, item_type, inner_key)
		if !ok {
			return nil
		}

		// 解包指针
		field_is_ptr := field_type.Kind() == reflect.Ptr
		if field_is_ptr {
			field_type = field_type.Elem()
		}

		// 创建实例并写入
		child := reflect.New(field_type)
		child_value := child.Elem()
		err = parseHits(hits, child_value, field_type)
		if err != nil {
			return err
		}

		// 如果字段带指针直接使用child
		if field_is_ptr {
			field_value.Set(child)
			return nil
		}

		field_value.Set(child_value)
	default:
		panic(fmt.Sprintf("不支持的类型: %s", kind))
	}
	return nil
}

// 搜索结构体字段用于innerHits, 首选tag > 次选tag > 名称相等, 返回(字段反射值,字段类型,是否找到)
func searchStructFieldWithInnerHit(item_value reflect.Value, item_type reflect.Type, key string) (out_value reflect.Value, out_type reflect.Type, ok bool) {
	// 搜索函数, 根据比较函数返回并设置结果
	searchFn := func(compareFn func(field *reflect.StructField) bool) bool {
		field_num := item_type.NumField()
		for i := 0; i < field_num; i++ {
			field := item_type.Field(i)
			// 忽略未导出字段
			if field.PkgPath != "" {
				continue
			}

			// 检查
			if compareFn(&field) {
				out_value, out_type, ok = item_value.Field(i), field.Type, true
				return true
			}
		}
		return false
	}

	compareFns := []func(field *reflect.StructField) bool{
		// 搜索首选tag
		func(field *reflect.StructField) bool { return field.Tag.Get(InnerHitsTagName) == key },
		// 搜索次选tag
		func(field *reflect.StructField) bool { return field.Tag.Get(InnerHitsSecondTagName) == key },
		// 检查名称等于key
		func(field *reflect.StructField) bool { return field.Name == key },
	}
	for _, compareFn := range compareFns {
		if searchFn(compareFn) {
			return
		}
	}
	return
}

// 搜索获取总数
func SearchTotal(ss *elastic.SearchService, timeout ...time.Duration) (int, error) {
	ctx, cancel := makeTimeoutCtx(timeout...)
	defer cancel()
	return SearchTotalWithCtx(ctx, ss)
}

// 搜索获取总数, 功能同SearchTotal
func SearchTotalWithCtx(ctx context.Context, ss *elastic.SearchService) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 由于只要总数, 所以不需要数据
	ss.Size(0)

	// 执行
	resp, err := ss.Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("在搜索时出现错误: %s", err.Error())
	}

	return int(resp.TotalHits()), nil
}
