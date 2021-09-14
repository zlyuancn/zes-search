package zes_search

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic/v7"
)

// 滚动获取, 功能同Scroll
func ScrollWithTimeout(scrollTimeout time.Duration, ss *elastic.ScrollService, batchTimeout time.Duration,
	bean interface{}, process func(total, index int, hit *elastic.SearchHit, bean interface{}) error) (total int, err error) {
	ctx, cancel := makeTimeoutCtx(scrollTimeout)
	defer cancel()
	return Scroll(ctx, ss, batchTimeout, bean, process)
}

// 滚动获取
//
// ctx 表示滚动期间上下文, 可用于滚动超时
// ss 表示设置好条件的滚动服务, 需要用户在滚动结束后自行调用 ss.Clear(context.Background())
// batchTimeout 表示每次滚动超时
// bean 表示接收数据的变量, 它必须是指向 struct, map, interface{} 的指针
// process 表示对每个数据如何处理
func Scroll(ctx context.Context, ss *elastic.ScrollService, batchTimeout time.Duration,
	bean interface{}, process func(total, index int, hit *elastic.SearchHit, bean interface{}) error) (total int, err error) {
	// 检查bean类型
	beanType := reflect.TypeOf(bean)
	if beanType.Kind() != reflect.Ptr {
		panic(errors.New("bean必须是一个指针"))
	}
	beanType = beanType.Elem()
	checkItemType(beanType)

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
			item := reflect.New(beanType)
			itemValue := item.Elem()

			// 将 hit 的 _source 解析到 child
			if err = jsoniter.Unmarshal(hit.Source, item.Interface()); err != nil {
				return 0, fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
			}

			// 附加, 解析所有 innerHits 到 childValue
			if err = parseAllInnerHits(hit, itemValue, beanType); err != nil {
				return 0, err
			}

			if err = process(total, index, hit, item.Interface()); err != nil {
				return 0, err
			}
			index++
		}
	}
}
