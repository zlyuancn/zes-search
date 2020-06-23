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
    "errors"
    "fmt"
    "reflect"
    "time"

    jsoniter "github.com/json-iterator/go"
    "github.com/olivere/elastic/v7"
)

var ErrNoDocuments = errors.New("没有匹配的结果")

func makeTimeoutCtx(timeout ...time.Duration) (context.Context, context.CancelFunc) {
    if len(timeout) == 0 || timeout[0] <= 0 {
        return context.WithCancel(context.Background())
    }

    return context.WithTimeout(context.Background(), timeout[0])
}

// 搜索, 将结果写入a, timeout为0时不设置超时
func Search(ss *elastic.SearchService, a interface{}, timeout ...time.Duration) (total int, err error) {
    ctx, cancel := makeTimeoutCtx(timeout...)
    defer cancel()
    return SearchWithCtx(ctx, ss, a)
}

// 功能同Search
func SearchWithCtx(ctx context.Context, ss *elastic.SearchService, a interface{}) (total int, err error) {
    if ctx == nil {
        ctx = context.Background()
    }

    a_value, a_type, is_search_one := checkOutParam(a)
    if is_search_one {
        ss = ss.Size(1)
    }

    resp, err := ss.Do(ctx)
    if err != nil {
        return 0, err
    }

    total = int(resp.TotalHits())
    if total == 0 || len(resp.Hits.Hits) == 0 {
        if is_search_one {
            return total, ErrNoDocuments
        }
        return total, nil
    }

    if err = parseHits(resp.Hits.Hits, a_value, a_type); err != nil {
        return total, err
    }
    return total, nil
}

// 搜索, 返回匹配结果的id列表, timeout为0时不设置超时
func SearchIds(ss *elastic.SearchService, timeout ...time.Duration) ([]string, int, error) {
    ctx, cancel := makeTimeoutCtx(timeout...)
    defer cancel()
    return SearchIdsWithCtx(ctx, ss)
}

// 功能同SearchIds
func SearchIdsWithCtx(ctx context.Context, ss *elastic.SearchService) ([]string, int, error) {
    if ctx == nil {
        ctx = context.Background()
    }

    ss.FetchSourceContext(elastic.NewFetchSourceContext(false))
    resp, err := ss.Do(ctx)
    if err != nil {
        return nil, 0, fmt.Errorf("在搜索时出现错误: %s", err.Error())
    }

    total := int(resp.TotalHits())
    if total == 0 || len(resp.Hits.Hits) == 0 {
        return nil, total, nil
    }

    out := make([]string, len(resp.Hits.Hits))
    for i, hit := range resp.Hits.Hits {
        out[i] = hit.Id
    }
    return out, total, nil
}

func parseHits(hits []*elastic.SearchHit, a_value reflect.Value, a_type reflect.Type) (err error) {
    if len(hits) == 0 {
        return nil
    }

    if a_type.Kind() != reflect.Slice {
        return parseOneHit(hits[0], a_value, a_type)
    }

    item_type, item_is_ptr := checkSliceType(a_type)

    items := make([]reflect.Value, len(hits))
    for i, hit := range hits {
        child := reflect.New(item_type)
        child_value := child.Elem()

        if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
            return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
        }

        if err = parseInnerHits(hit, child_value, item_type); err != nil {
            return err
        }

        if item_is_ptr {
            child_value = child
        }
        items[i] = child_value
    }

    values := reflect.Append(a_value, items...)
    a_value.Set(values)
    return nil
}

func parseOneHit(hit *elastic.SearchHit, item_value reflect.Value, item_type reflect.Type) (err error) {
    checkItemType(item_type)

    child := reflect.New(item_type)
    child_value := child.Elem()

    if err = jsoniter.Unmarshal(hit.Source, child.Interface()); err != nil {
        return fmt.Errorf("解码失败<%s>: %s", hit.Id, err.Error())
    }

    if err = parseInnerHits(hit, child_value, item_type); err != nil {
        return err
    }

    item_value.Set(child_value)
    return nil
}

// 检查输出参数, 它必须是一个已初始化的指针
func checkOutParam(a interface{}) (a_value reflect.Value, a_type reflect.Type, one bool) {
    a_type = reflect.TypeOf(a)
    if a_type.Kind() != reflect.Ptr {
        panic(errors.New("a必须是一个指针"))
    }

    a_type = a_type.Elem()
    a_value = reflect.ValueOf(a).Elem()

    if a_type.Kind() == reflect.Invalid {
        panic(errors.New("a是无效的, 它可能未初始化"))
    }

    one = a_type.Kind() != reflect.Slice
    return
}

// 检查切片类型
func checkSliceType(slice_type reflect.Type) (item_type reflect.Type, item_is_ptr bool) {
    item_type = slice_type.Elem()
    item_is_ptr = item_type.Kind() == reflect.Ptr
    if item_is_ptr {
        item_type = item_type.Elem()
    }
    checkItemType(item_type)
    return
}

// 检查项目类型, 它必须是 interface{}, map 或 struct
func checkItemType(item_type reflect.Type) {
    item_kind := item_type.Kind()
    if item_kind != reflect.Interface && item_kind != reflect.Map && item_kind != reflect.Struct {
        panic(errors.New("item必须是指向interface{}, map 或 struct的指针"))
    }
}

func parseInnerHits(hit *elastic.SearchHit, item_value reflect.Value, item_type reflect.Type) (err error) {
    if len(hit.InnerHits) == 0 {
        return nil
    }

    for inner_key, inner_value := range hit.InnerHits {
        if err = parseInnerHit(inner_key, inner_value, item_value, item_type); err != nil {
            return err
        }
    }
    return nil
}

func parseInnerHit(inner_key string, hit *elastic.SearchHitInnerHits, item_value reflect.Value, item_type reflect.Type) (err error) {
    if hit == nil || hit.Hits == nil || len(hit.Hits.Hits) == 0 {
        return nil
    }

    is_interface := item_type.Kind() == reflect.Interface
    if is_interface {
        item_value = item_value.Elem()
        item_type = item_value.Type()
    }
    checkItemType(item_type)

    hits := hit.Hits.Hits
    switch kind := item_type.Kind(); kind {
    case reflect.Map:
        data := []map[string]interface{}{}
        child_value := reflect.ValueOf(&data).Elem()
        child_type := reflect.TypeOf(&data).Elem()
        err = parseHits(hits, child_value, child_type)
        if err != nil {
            return err
        }
        item_value.SetMapIndex(reflect.ValueOf(inner_key), child_value)
    case reflect.Struct:
        field_value, field_type, ok := searchStructField(item_value, item_type, inner_key)
        if !ok {
            return nil
        }

        field_is_ptr := field_type.Kind() == reflect.Ptr
        if field_is_ptr {
            field_type = field_type.Elem()
        }

        child := reflect.New(field_type)
        child_value := child.Elem()
        err = parseHits(hits, child_value, field_type)
        if err != nil {
            return err
        }

        if field_is_ptr {
            child_value = child
        }

        field_value.Set(child_value)
    default:
        panic(fmt.Sprintf("不支持的类型: %s", kind))
    }
    return nil
}

func searchStructField(item_value reflect.Value, item_type reflect.Type, key string) (reflect.Value, reflect.Type, bool) {
    field_num := item_type.NumField()
    for i := 0; i < field_num; i++ {
        field := item_type.Field(i)
        if field.PkgPath != "" {
            continue
        }
        if field.Tag.Get("json") == key || field.Name == key {
            return item_value.Field(i), field.Type, true
        }
    }
    return reflect.Value{}, nil, false
}

// 搜索获取总数
func SearchTotal(ss *elastic.SearchService, timeout ...time.Duration) (int, error) {
    ctx, cancel := makeTimeoutCtx(timeout...)
    defer cancel()
    return SearchTotalWithCtx(ctx, ss)
}

func SearchTotalWithCtx(ctx context.Context, ss *elastic.SearchService) (int, error) {
    if ctx == nil {
        ctx = context.Background()
    }

    ss.Size(0)
    resp, err := ss.Do(ctx)
    if err != nil {
        return 0, fmt.Errorf("在搜索时出现错误: %s", err.Error())
    }

    return int(resp.TotalHits()), nil
}
