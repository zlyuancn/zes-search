package zes_search

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

// 搜索获取总数, 功能同SearchTotal
func SearchTotalWithTimeout(ss *elastic.SearchService, timeout time.Duration) (int, error) {
	ctx, cancel := makeTimeoutCtx(timeout)
	defer cancel()
	return SearchTotal(ctx, ss)
}

// 搜索获取总数
func SearchTotal(ctx context.Context, ss *elastic.SearchService) (int, error) {
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
