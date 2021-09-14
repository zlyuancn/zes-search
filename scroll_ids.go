package zes_search

import (
	"context"
	"io"
	"time"

	"github.com/olivere/elastic/v7"
)

// 滚动获取ids, 功能同ScrollIds
func ScrollIdsWithTimeout(scrollTimeout time.Duration, ss *elastic.ScrollService, batchTimeout time.Duration,
	process func(total, index int, hit *elastic.SearchHit, id string) error) (total int, err error) {
	ctx, cancel := makeTimeoutCtx(scrollTimeout)
	defer cancel()
	return ScrollIds(ctx, ss, batchTimeout, process)
}

// 滚动获取ids
//
// ctx 表示滚动期间上下文, 可用于滚动超时
// ss 表示设置好条件的滚动服务, 需要用户在滚动结束后自行调用 ss.Clear(context.Background())
// batchTimeout 表示每次滚动超时
// process 表示对每个id如何处理
func ScrollIds(ctx context.Context, ss *elastic.ScrollService, batchTimeout time.Duration,
	process func(total, index int, hit *elastic.SearchHit, id string) error) (total int, err error) {
	// 获取id不需要 _source
	ss.FetchSourceContext(elastic.NewFetchSourceContext(false))

	var index int
	for {
		// 执行
		resp, err := func() (*elastic.SearchResult, error) {
			batchCtx, cancel := makeTimeoutCtxWithBaseCtx(ctx, batchTimeout) // 每次滚动的上下文
			defer cancel()
			return ss.Do(batchCtx)
		}()
		if err == io.EOF { // 滚动完成
			return total, nil
		}
		if err != nil {
			return 0, err
		}

		// 获取总数
		total = int(resp.TotalHits())
		if total == 0 || resp.Hits == nil {
			continue
		}

		// 遍历每次滚动收到的数据
		for _, hit := range resp.Hits.Hits {
			if err = process(total, index, hit, hit.Id); err != nil {
				return 0, err
			}
			index++
		}
	}
}
