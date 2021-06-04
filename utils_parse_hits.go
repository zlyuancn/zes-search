package zes_search

import (
	"fmt"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic/v7"
)

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
