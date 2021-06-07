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
	"time"

	"github.com/olivere/elastic/v7"
)

// 搜索, 功能同Search
func SearchWithTimeout(timeout time.Duration, ss *elastic.SearchService, a interface{}) (total int, err error) {
	ctx, cancel := makeTimeoutCtx(timeout)
	defer cancel()
	return Search(ctx, ss, a)
}

// 搜索, 将结果写入a
func Search(ctx context.Context, ss *elastic.SearchService, a interface{}) (total int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 检查输出参数, 它必须是一个已初始化的指针, 返回(指向的value, 指向的type, 是否非切片)
	aValue, aType, isSearchOne := checkOutParam(a)
	if isSearchOne { // 如果不是切片, 表示只要一条数据
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
		if isSearchOne { // 如果只要一条数据, 但是无数据则返回错误
			return total, ErrNoDocuments
		}
		return total, nil
	}

	// 将数据写入a
	if err = parseHits(resp.Hits.Hits, aValue, aType); err != nil {
		return total, err
	}
	return total, nil
}
