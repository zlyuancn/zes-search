package zes_search

import (
	"fmt"
	"reflect"

	"github.com/olivere/elastic/v7"
)

// 解析innerHits的标签名
const (
	InnerHitsTagName       = "inner_hits"
	InnerHitsSecondTagName = "json" // 第二优先级
)

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
