package temp

import (
	"Distributed_Object_Storage/src/lib/rs"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// 处理HTTP请求，获取URL中的token，并从token中获取可恢复上传流
func head(w http.ResponseWriter, r *http.Request) {
	// 从URL中获取token
	token := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 从token中获取可恢复上传流
	stream, e := rs.NewRSResumablePutStreamFromToken(token)
	// 如果获取流失败，则返回403 Forbidden
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// 获取当前流的大小
	current := stream.CurrentSize()
	// 如果流不存在，则返回404 Not Found
	if current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 设置响应头中的content-length为当前流的大小
	w.Header().Set("content-length", fmt.Sprintf("%d", current))
}
