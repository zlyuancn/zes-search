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
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

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
