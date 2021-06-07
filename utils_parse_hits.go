package zes_search

import (
	"fmt"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic/v7"
)

// 解析 hits 并写入值
func parseHits(hits []*elastic.SearchHit, aValue reflect.Value, aType reflect.Type) (err error) {
	if len(hits) == 0 {
		return nil
	}

	// 如果非切片, 表示只要一条数据
	if aType.Kind() != reflect.Slice {
		return parseOneHit(hits[0], aValue, aType)
	}

	// 检查切片类型, 返回(item类型,item是否带指针)
	itemType, itemIsPtr := checkSliceType(aType)

	// 遍历hits并写入对象中
	items := make([]reflect.Value, len(hits))
	for i, hit := range hits {
		child := reflect.New(itemType)
		childValue := child.Elem()

		// 将 hit 的 _source 解析到 child
		if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
			return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
		}

		// 附加, 解析所有 innerHits 到 childValue
		if err = parseAllInnerHits(hit, childValue, itemType); err != nil {
			return err
		}

		// 如果item带指针直接使用child
		if itemIsPtr {
			items[i] = child
			continue
		}

		items[i] = childValue
	}

	values := reflect.Append(aValue, items...)
	aValue.Set(values)
	return nil
}

// 解析一个 hit 并写入值
func parseOneHit(hit *elastic.SearchHit, itemValue reflect.Value, itemType reflect.Type) (err error) {
	checkItemType(itemType)

	child := reflect.New(itemType)
	childValue := child.Elem()

	// 将 hit 的 _source 解析到 child
	if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
		return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
	}

	// 附加, 解析所有 innerHits 到 childValue
	if err = parseAllInnerHits(hit, childValue, itemType); err != nil {
		return err
	}

	itemValue.Set(childValue)
	return nil
}
