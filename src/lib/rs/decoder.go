package rs

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

type decoder struct {
	readers   []io.Reader
	writers   []io.Writer
	enc       reedsolomon.Encoder
	size      int64
	cache     []byte
	cacheSize int
	total     int64
}

func NewDecoder(readers []io.Reader, writers []io.Writer, size int64) *decoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	return &decoder{readers, writers, enc, size, nil, 0, 0}
}

// 从decoder中读取数据到p中
func (d *decoder) Read(p []byte) (n int, err error) {
	// 如果缓存大小为0，则从数据源中获取数据
	if d.cacheSize == 0 {
		e := d.getData()
		// 如果获取数据失败，则返回错误
		if e != nil {
			return 0, e
		}
	}
	// 获取p的长度
	length := len(p)
	// 如果缓存大小小于p的长度，则将p的长度设置为缓存大小
	if d.cacheSize < length {
		length = d.cacheSize
	}
	// 缓存大小减去p的长度
	d.cacheSize -= length
	// 将缓存中的数据复制到p中
	copy(p, d.cache[:length])
	// 将缓存中的数据截断
	d.cache = d.cache[length:]
	// 返回读取的字节数和nil
	return length, nil
}

// getData函数用于从多个读取器中读取数据，并使用编码器进行重构，然后将重构后的数据写入多个写入器中，并将数据缓存起来
func (d *decoder) getData() error {
	// 如果已经读取了所有数据，则返回EOF错误
	if d.total == d.size {
		return io.EOF
	}
	// 创建一个切片，用于存储每个分片的数据
	shards := make([][]byte, ALL_SHARDS)
	// 创建一个切片，用于存储需要修复的分片ID
	repairIds := make([]int, 0)
	// 遍历每个分片
	for i := range shards {
		// 如果当前分片的读取器为空，则将其ID添加到需要修复的分片ID切片中
		if d.readers[i] == nil {
			repairIds = append(repairIds, i)
		} else {
			// 创建一个切片，用于存储当前分片的数据
			shards[i] = make([]byte, BLOCK_PER_SHARD)
			// 从当前分片的读取器中读取数据
			n, e := io.ReadFull(d.readers[i], shards[i])
			// 如果读取过程中出现错误，并且错误不是EOF和ErrUnexpectedEOF，则将当前分片的数据置为nil
			if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
				shards[i] = nil
				// 如果读取的数据量不等于BLOCK_PER_SHARD，则将当前分片的数据截断为实际读取的数据量
			} else if n != BLOCK_PER_SHARD {
				shards[i] = shards[i][:n]
			}
		}
	}
	// 使用编码器重构数据
	e := d.enc.Reconstruct(shards)
	// 如果重构过程中出现错误，则返回错误
	if e != nil {
		return e
	}
	// 遍历需要修复的分片ID切片
	for i := range repairIds {
		// 获取当前分片的ID
		id := repairIds[i]
		// 将重构后的数据写入当前分片的写入器中
		d.writers[id].Write(shards[id])
	}
	// 遍历每个分片
	for i := 0; i < DATA_SHARDS; i++ {
		// 获取当前分片的数据量
		shardSize := int64(len(shards[i]))
		// 如果当前分片的数据量加上已经缓存的数据量超过了总数据量，则将当前分片的数据量调整为总数据量减去已经缓存的数据量
		if d.total+shardSize > d.size {
			shardSize -= d.total + shardSize - d.size
		}
		// 将当前分片的数据添加到缓存中
		d.cache = append(d.cache, shards[i][:shardSize]...)
		// 更新缓存的大小
		d.cacheSize += int(shardSize)
		// 更新已经读取的数据量
		d.total += shardSize
	}
	// 返回nil，表示读取成功
	return nil
}
