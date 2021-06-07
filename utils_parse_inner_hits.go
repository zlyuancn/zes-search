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
func parseAllInnerHits(hit *elastic.SearchHit, itemValue reflect.Value, itemType reflect.Type) (err error) {
	if len(hit.InnerHits) == 0 {
		return nil
	}

	for innerKey, innerHits := range hit.InnerHits {
		if err = parseInnerHit(innerKey, innerHits, itemValue, itemType); err != nil {
			return err
		}
	}
	return nil
}

// 解析一个 innerHits 到 item
func parseInnerHit(innerKey string, innerHits *elastic.SearchHitInnerHits, itemValue reflect.Value, itemType reflect.Type) (err error) {
	if innerHits == nil || innerHits.Hits == nil || len(innerHits.Hits.Hits) == 0 {
		return nil
	}

	// interface 解包
	isInterface := itemType.Kind() == reflect.Interface
	if isInterface {
		itemValue = itemValue.Elem()
		itemType = itemValue.Type()
	}
	checkItemType(itemType)

	// 开始解析
	hits := innerHits.Hits.Hits
	switch kind := itemType.Kind(); kind {
	case reflect.Map: // 如果是一个map, 那么用户无法指定inner_hits类型
		var data []map[string]interface{}
		childValue := reflect.ValueOf(&data).Elem() // 获取指针的反射值
		childType := reflect.TypeOf(&data).Elem()   // 获取指针的的反射类型
		err = parseHits(hits, childValue, childType)
		if err != nil {
			return err
		}
		itemValue.SetMapIndex(reflect.ValueOf(innerKey), childValue)
	case reflect.Struct:
		// 获取结构体的指定字段
		fieldValue, fieldType, ok := searchStructFieldWithInnerHit(itemValue, itemType, innerKey)
		if !ok {
			return nil
		}

		// 解包指针
		fieldIsPtr := fieldType.Kind() == reflect.Ptr
		if fieldIsPtr {
			fieldType = fieldType.Elem()
		}

		// 创建实例并写入
		child := reflect.New(fieldType)
		childValue := child.Elem()
		err = parseHits(hits, childValue, fieldType)
		if err != nil {
			return err
		}

		// 如果字段带指针直接使用child
		if fieldIsPtr {
			fieldValue.Set(child)
			return nil
		}

		fieldValue.Set(childValue)
	default:
		panic(fmt.Sprintf("不支持的类型: %s", kind))
	}
	return nil
}

// 搜索结构体字段用于innerHits, 首选tag > 次选tag > 名称相等, 返回(字段反射值,字段类型,是否找到)
func searchStructFieldWithInnerHit(itemValue reflect.Value, itemType reflect.Type, key string) (outValue reflect.Value, outType reflect.Type, ok bool) {
	// 搜索函数, 根据比较函数返回并设置结果
	searchFn := func(compareFn func(field *reflect.StructField) bool) bool {
		fieldNum := itemType.NumField()
		for i := 0; i < fieldNum; i++ {
			field := itemType.Field(i)
			// 忽略未导出字段
			if field.PkgPath != "" {
				continue
			}

			// 检查
			if compareFn(&field) {
				outValue, outType, ok = itemValue.Field(i), field.Type, true
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
