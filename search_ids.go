package zes_search

import (
	"context"
	"time"

	"github.com/olivere/elastic/v7"
)

// 搜索ids, 功能同SearchIds
func SearchIdsWithTimeout(timeout time.Duration, ss *elastic.SearchService) (ids []string, resp *elastic.SearchResult, total int, err error) {
	ctx, cancel := makeTimeoutCtx(timeout)
	defer cancel()
	return SearchIds(ctx, ss)
}

// 搜索ids, 返回匹配结果的id列表
func SearchIds(ctx context.Context, ss *elastic.SearchService) (ids []string, resp *elastic.SearchResult, total int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 获取id不需要 _source
	ss.FetchSourceContext(elastic.NewFetchSourceContext(false))

	// 执行
	resp, err = ss.Do(ctx)
	if err != nil {
		return
	}

	// 获取总数
	total = int(resp.TotalHits())
	if total == 0 || resp.Hits == nil || len(resp.Hits.Hits) == 0 {
		return nil, resp, total, nil
	}

	// 写入id
	ids = make([]string, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		ids[i] = hit.Id
	}
	return
}
