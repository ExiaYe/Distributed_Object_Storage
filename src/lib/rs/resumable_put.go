package rs

import (
	"Distributed_Object_Storage/src/lib/objectstream"
	"Distributed_Object_Storage/src/lib/utils"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

type resumableToken struct {
	Name    string
	Size    int64
	Hash    string
	Servers []string
	Uuids   []string
}

type RSResumablePutStream struct {
	*RSPutStream
	*resumableToken
}

// 创建一个新的RSResumablePutStream
func NewRSResumablePutStream(dataServers []string, name, hash string, size int64) (*RSResumablePutStream, error) {
	// 创建一个新的RSPutStream
	putStream, e := NewRSPutStream(dataServers, hash, size)
	if e != nil {
		// 如果创建失败，返回错误
		return nil, e
	}
	// 创建一个uuids切片，长度为ALL_SHARDS
	uuids := make([]string, ALL_SHARDS)
	// 遍历uuids切片
	for i := range uuids {
		// 将putStream的writers中的第i个元素的Uuid赋值给uuids的第i个元素
		uuids[i] = putStream.writers[i].(*objectstream.TempPutStream).Uuid
	}
	// 创建一个resumableToken，包含name、size、hash、dataServers、uuids
	token := &resumableToken{name, size, hash, dataServers, uuids}
	// 返回RSResumablePutStream，包含putStream和token
	return &RSResumablePutStream{putStream, token}, nil
}

// 根据给定的token创建一个新的RSResumablePutStream
func NewRSResumablePutStreamFromToken(token string) (*RSResumablePutStream, error) {
	// 将token进行base64解码
	b, e := base64.StdEncoding.DecodeString(token)
	if e != nil {
		return nil, e
	}

	// 将解码后的token解析为resumableToken结构体
	var t resumableToken
	e = json.Unmarshal(b, &t)
	if e != nil {
		return nil, e
	}

	// 创建一个io.Writer切片，用于存储每个分片的TempPutStream
	writers := make([]io.Writer, ALL_SHARDS)
	for i := range writers {
		// 创建每个分片的TempPutStream
		writers[i] = &objectstream.TempPutStream{t.Servers[i], t.Uuids[i]}
	}
	// 创建一个Encoder，用于将数据写入io.Writer切片
	enc := NewEncoder(writers)
	// 返回一个新的RSResumablePutStream
	return &RSResumablePutStream{&RSPutStream{enc}, &t}, nil
}

func (s *RSResumablePutStream) ToToken() string {
	b, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(b)
}

// 获取当前文件大小
func (s *RSResumablePutStream) CurrentSize() int64 {
	// 发送HTTP HEAD请求，获取文件大小
	r, e := http.Head(fmt.Sprintf("http://%s/temp/%s", s.Servers[0], s.Uuids[0]))
	if e != nil {
		// 如果请求失败，打印错误信息并返回-1
		log.Println(e)
		return -1
	}
	if r.StatusCode != http.StatusOK {
		// 如果返回状态码不是200，打印状态码并返回-1
		log.Println(r.StatusCode)
		return -1
	}
	// 从响应头中获取文件大小
	size := utils.GetSizeFromHeader(r.Header) * DATA_SHARDS
	// 如果文件大小大于指定大小，则取指定大小
	if size > s.Size {
		size = s.Size
	}
	// 返回文件大小
	return size
}
