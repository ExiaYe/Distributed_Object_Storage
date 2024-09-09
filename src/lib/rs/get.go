package rs

import (
	"Distributed_Object_Storage/src/lib/objectstream"
	"fmt"
	"io"
)

type RSGetStream struct {
	*decoder
}

// NewRSGetStream 创建一个新的RSGetStream，用于从多个数据服务器中获取数据
func NewRSGetStream(locateInfo map[int]string, dataServers []string, hash string, size int64) (*RSGetStream, error) {
	// 检查locateInfo和dataServers的长度是否等于ALL_SHARDS
	if len(locateInfo)+len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}

	// 创建一个io.Reader切片，用于存储从数据服务器中获取的数据
	readers := make([]io.Reader, ALL_SHARDS)
	// 遍历locateInfo，获取数据
	for i := 0; i < ALL_SHARDS; i++ {
		server := locateInfo[i]
		// 如果locateInfo中的数据为空，则从dataServers中获取数据
		if server == "" {
			locateInfo[i] = dataServers[0]
			dataServers = dataServers[1:]
			continue
		}
		// 从数据服务器中获取数据
		reader, e := objectstream.NewGetStream(server, fmt.Sprintf("%s.%d", hash, i))
		// 如果获取数据成功，则将数据存储到readers中
		if e == nil {
			readers[i] = reader
		}
	}

	// 创建一个io.Writer切片，用于存储写入的数据
	writers := make([]io.Writer, ALL_SHARDS)
	// 计算每个分片的大小
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	var e error
	// 遍历readers，将数据写入到writers中
	for i := range readers {
		// 如果readers中的数据为空，则创建一个临时存储流
		if readers[i] == nil {
			writers[i], e = objectstream.NewTempPutStream(locateInfo[i], fmt.Sprintf("%s.%d", hash, i), perShard)
			// 如果创建临时存储流失败，则返回错误
			if e != nil {
				return nil, e
			}
		}
	}

	// 创建一个Decoder，用于解码数据
	dec := NewDecoder(readers, writers, size)
	// 返回RSGetStream
	return &RSGetStream{dec}, nil
}

// Close方法用于关闭RSGetStream
func (s *RSGetStream) Close() {
	// 遍历s.writers
	for i := range s.writers {
		// 如果s.writers[i]不为nil
		if s.writers[i] != nil {
			// 调用s.writers[i]的Commit方法，并传入true
			s.writers[i].(*objectstream.TempPutStream).Commit(true)
		}
	}
}

// Seek方法用于将流中的位置移动到指定的偏移量
func (s *RSGetStream) Seek(offset int64, whence int) (int64, error) {
	// 如果whence参数不是io.SeekCurrent，则抛出异常
	if whence != io.SeekCurrent {
		panic("only support SeekCurrent")
	}
	// 如果offset参数小于0，则抛出异常
	if offset < 0 {
		panic("only support forward seek")
	}
	// 当offset不为0时，循环执行以下操作
	for offset != 0 {
		// 定义每次读取的长度为BLOCK_SIZE
		length := int64(BLOCK_SIZE)
		// 如果offset小于length，则将length设置为offset
		if offset < length {
			length = offset
		}
		// 创建一个长度为length的字节数组
		buf := make([]byte, length)
		// 从流中读取length长度的数据到buf中
		io.ReadFull(s, buf)
		// 将offset减去length
		offset -= length
	}
	// 返回offset和nil
	return offset, nil
}
