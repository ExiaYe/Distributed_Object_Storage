package main

import (
	"Distributed_Object_Storage/src/lib/es"
	"log"
)

const MIN_VERSION_COUNT = 5

func main() {
	// 调用es.SearchVersionStatus函数，传入参数MIN_VERSION_COUNT + 1，获取版本状态
	buckets, e := es.SearchVersionStatus(MIN_VERSION_COUNT + 1)
	// 如果有错误，打印错误信息并返回
	if e != nil {
		log.Println(e)
		return
	}
	// 遍历buckets
	for i := range buckets {
		bucket := buckets[i]
		// 遍历bucket中的Doc_count - MIN_VERSION_COUNT
		for v := 0; v < bucket.Doc_count-MIN_VERSION_COUNT; v++ {
			// 调用es.DelMetadata函数，删除metadata
			es.DelMetadata(bucket.Key, v+int(bucket.Min_version.Value))
		}
	}
}
