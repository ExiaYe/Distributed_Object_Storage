package objects

import (
	"net/http"
	"strings"
)

// 处理GET请求
func get(w http.ResponseWriter, r *http.Request) {
	// 获取URL中的文件名
	file := getFile(strings.Split(r.URL.EscapedPath(), "/")[2])
	// 如果文件不存在，返回404错误
	if file == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 发送文件
	sendFile(w, file)
}
