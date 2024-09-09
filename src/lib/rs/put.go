package rs

import (
	"Distributed_Object_Storage/src/lib/objectstream"
	"fmt"
	"io"
)

type RSPutStream struct {
	*encoder
}

// NewRSPutStream 创建一个新的RSPutStream，用于将数据写入多个数据服务器
func NewRSPutStream(dataServers []string, hash string, size int64) (*RSPutStream, error) {
	// 检查数据服务器数量是否与ALL_SHARDS相等
	if len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}

	// 计算每个分片的大小
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	// 创建一个io.Writer切片，用于存储每个分片的写入器
	writers := make([]io.Writer, ALL_SHARDS)
	var e error
	// 遍历每个分片，创建对应的写入器
	for i := range writers {
		// 创建一个临时的写入器
		writers[i], e = objectstream.NewTempPutStream(dataServers[i],
			fmt.Sprintf("%s.%d", hash, i), perShard)
		// 如果创建失败，返回错误
		if e != nil {
			return nil, e
		}
	}
	// 创建一个编码器，用于将数据写入多个写入器
	enc := NewEncoder(writers)

	// 返回RSPutStream
	return &RSPutStream{enc}, nil
}

func (s *RSPutStream) Commit(success bool) {
	s.Flush()
	for i := range s.writers {
		s.writers[i].(*objectstream.TempPutStream).Commit(success)
	}
}
