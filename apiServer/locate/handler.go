package locate

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Handler函数用于处理HTTP请求
func Handler(w http.ResponseWriter, r *http.Request) {
	// 获取请求的方法
	m := r.Method
	// 如果请求的方法不是GET，则返回405 Method Not Allowed
	if m != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// 获取请求的URL路径，并分割成数组
	info := Locate(strings.Split(r.URL.EscapedPath(), "/")[2])
	// 如果获取的信息为空，则返回404 Not Found
	if len(info) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 将获取的信息转换为JSON格式
	b, _ := json.Marshal(info)
	// 将JSON格式的信息写入响应
	w.Write(b)
}
