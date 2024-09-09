package rs

import (
	"Distributed_Object_Storage/src/lib/objectstream"
	"io"
)

type RSResumableGetStream struct {
	*decoder
}

// 创建一个新的RSResumableGetStream，用于从多个数据服务器中获取数据
func NewRSResumableGetStream(dataServers []string, uuids []string, size int64) (*RSResumableGetStream, error) {
	// 创建一个io.Reader切片，用于存储从每个数据服务器中获取的数据
	readers := make([]io.Reader, ALL_SHARDS)
	var e error
	// 遍历每个数据服务器
	for i := 0; i < ALL_SHARDS; i++ {
		// 从每个数据服务器中获取数据
		readers[i], e = objectstream.NewTempGetStream(dataServers[i], uuids[i])
		// 如果获取数据失败，则返回错误
		if e != nil {
			return nil, e
		}
	}
	// 创建一个io.Writer切片，用于存储写入的数据
	writers := make([]io.Writer, ALL_SHARDS)
	// 创建一个Decoder，用于解码数据
	dec := NewDecoder(readers, writers, size)
	// 返回RSResumableGetStream
	return &RSResumableGetStream{dec}, nil
}
