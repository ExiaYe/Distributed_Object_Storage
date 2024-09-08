package objects

import (
	"Distributed_Object_Storage/src/lib/es"
	"Distributed_Object_Storage/src/lib/utils"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// 处理GET请求
func get(w http.ResponseWriter, r *http.Request) {
	// 获取URL中的文件名
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 获取URL中的版本号
	versionId := r.URL.Query()["version"]
	// 初始化版本号为0
	version := 0
	// 初始化错误变量
	var e error
	// 如果URL中有版本号
	if len(versionId) != 0 {
		// 将版本号转换为整数
		version, e = strconv.Atoi(versionId[0])
		// 如果转换失败
		if e != nil {
			// 打印错误信息
			log.Println(e)
			// 返回错误状态码
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	// 获取文件的元数据
	meta, e := es.GetMetadata(name, version)
	// 如果获取元数据失败
	if e != nil {
		// 打印错误信息
		log.Println(e)
		// 返回错误状态码
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 如果文件的哈希值为空
	if meta.Hash == "" {
		// 返回错误状态码
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 对文件的哈希值进行URL编码
	hash := url.PathEscape(meta.Hash)
	// 获取文件的流
	stream, e := GetStream(hash, meta.Size)
	// 如果获取文件流失败
	if e != nil {
		// 打印错误信息
		log.Println(e)
		// 返回错误状态码
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 获取请求头中的偏移量
	offset := utils.GetOffsetFromHeader(r.Header)
	// 如果偏移量不为0
	if offset != 0 {
		// 将流移动到偏移量位置
		stream.Seek(offset, io.SeekCurrent)
		// 设置响应头中的content-range
		w.Header().Set("content-range", fmt.Sprintf("bytes %d-%d/%d", offset, meta.Size-1, meta.Size))
		// 返回部分内容状态码
		w.WriteHeader(http.StatusPartialContent)
	}
	// 初始化是否接受gzip压缩的变量
	acceptGzip := false
	// 获取请求头中的Accept-Encoding
	encoding := r.Header["Accept-Encoding"]
	// 遍历Accept-Encoding
	for i := range encoding {
		// 如果Accept-Encoding中包含gzip
		if encoding[i] == "gzip" {
			// 设置是否接受gzip压缩为true
			acceptGzip = true
			break
		}
	}
	// 如果接受gzip压缩
	if acceptGzip {
		// 设置响应头中的content-encoding
		w.Header().Set("content-encoding", "gzip")
		// 创建gzip压缩的Writer
		w2 := gzip.NewWriter(w)
		// 将流复制到gzip压缩的Writer中
		io.Copy(w2, stream)
		// 关闭gzip压缩的Writer
		w2.Close()
	} else {
		// 将流复制到ResponseWriter中
		io.Copy(w, stream)
	}
	// 关闭流
	stream.Close()
}
