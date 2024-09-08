package temp

import (
	"Distributed_Object_Storage/apiServer/locate"
	"Distributed_Object_Storage/src/lib/es"
	"Distributed_Object_Storage/src/lib/rs"
	"Distributed_Object_Storage/src/lib/utils"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

// 处理PUT请求
func put(w http.ResponseWriter, r *http.Request) {
	// 从URL中获取token
	token := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 根据token创建一个新的可恢复上传流
	stream, e := rs.NewRSResumablePutStreamFromToken(token)
	// 如果创建失败，返回403 Forbidden
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// 获取当前上传流的大小
	current := stream.CurrentSize()
	// 如果当前大小为-1，表示上传流不存在，返回404 Not Found
	if current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 从请求头中获取偏移量
	offset := utils.GetOffsetFromHeader(r.Header)
	// 如果当前大小不等于偏移量，返回416 Requested Range Not Satisfiable
	if current != offset {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	// 创建一个大小为BLOCK_SIZE的字节数组
	bytes := make([]byte, rs.BLOCK_SIZE)
	// 循环读取请求体中的数据
	for {
		// 从请求体中读取数据
		n, e := io.ReadFull(r.Body, bytes)
		// 如果读取失败，且不是EOF和ErrUnexpectedEOF，返回500 Internal Server Error
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// 更新当前大小
		current += int64(n)
		// 如果当前大小大于上传流的大小，提交上传流，并返回403 Forbidden
		if current > stream.Size {
			stream.Commit(false)
			log.Println("resumable put exceed size")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// 如果读取的数据不等于BLOCK_SIZE且当前大小不等于上传流的大小，返回
		if n != rs.BLOCK_SIZE && current != stream.Size {
			return
		}
		// 将读取的数据写入上传流
		stream.Write(bytes[:n])
		// 如果当前大小等于上传流的大小，提交上传流
		if current == stream.Size {
			stream.Flush()
			// 根据上传流的服务器和UUID创建一个新的可恢复下载流
			getStream, e := rs.NewRSResumableGetStream(stream.Servers, stream.Uuids, stream.Size)
			// 计算下载流的哈希值
			hash := url.PathEscape(utils.CalculateHash(getStream))
			// 如果哈希值不等于上传流的哈希值，提交上传流，并返回403 Forbidden
			if hash != stream.Hash {
				stream.Commit(false)
				log.Println("resumable put done but hash mismatch")
				w.WriteHeader(http.StatusForbidden)
				return
			}
			// 如果哈希值已经存在，提交上传流，否则提交上传流并添加版本
			if locate.Exist(url.PathEscape(hash)) {
				stream.Commit(false)
			} else {
				stream.Commit(true)
			}
			// 添加版本
			e = es.AddVersion(stream.Name, stream.Hash, stream.Size)
			// 如果添加版本失败，返回500 Internal Server Error
			if e != nil {
				log.Println(e)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}
