package rs

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

type encoder struct {
	writers []io.Writer
	enc     reedsolomon.Encoder
	cache   []byte
}

func NewEncoder(writers []io.Writer) *encoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	return &encoder{writers, enc, nil}
}

// Write方法将p中的字节写入e中，并返回写入的字节数和错误信息
func (e *encoder) Write(p []byte) (n int, err error) {
	// 获取p的长度
	length := len(p)
	// 当前写入的位置
	current := 0
	// 当p中还有字节未写入时，继续循环
	for length != 0 {
		// 计算下一个要写入的字节数
		next := BLOCK_SIZE - len(e.cache)
		// 如果下一个要写入的字节数大于p中剩余的字节数，则将下一个要写入的字节数设置为p中剩余的字节数
		if next > length {
			next = length
		}
		// 将p中从current开始的next个字节追加到e.cache中
		e.cache = append(e.cache, p[current:current+next]...)
		// 如果e.cache中的字节数等于BLOCK_SIZE，则调用Flush方法将e.cache中的字节写入e中
		if len(e.cache) == BLOCK_SIZE {
			e.Flush()
		}
		// 更新current和length
		current += next
		length -= next
	}
	// 返回写入的字节数和nil
	return len(p), nil
}

// Flush函数用于将缓存中的数据写入到输出流中
func (e *encoder) Flush() {
	// 如果缓存中没有数据，则直接返回
	if len(e.cache) == 0 {
		return
	}
	// 将缓存中的数据分割成多个分片
	shards, _ := e.enc.Split(e.cache)
	// 将分片编码
	e.enc.Encode(shards)
	// 将编码后的分片写入到输出流中
	for i := range shards {
		e.writers[i].Write(shards[i])
	}
	// 清空缓存
	e.cache = []byte{}
}
