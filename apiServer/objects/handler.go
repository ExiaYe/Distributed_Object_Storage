package objects

import "net/http"

// Handler函数用于处理HTTP请求
func Handler(w http.ResponseWriter, r *http.Request) {
	// 获取请求的方法
	m := r.Method
	// 如果请求方法是POST
	if m == http.MethodPost {
		// 调用post函数处理POST请求
		post(w, r)
		return
	}
	// 如果请求方法是PUT
	if m == http.MethodPut {
		// 调用put函数处理PUT请求
		put(w, r)
		return
	}
	// 如果请求方法是GET
	if m == http.MethodGet {
		// 调用get函数处理GET请求
		get(w, r)
		return
	}
	// 如果请求方法是DELETE
	if m == http.MethodDelete {
		// 调用del函数处理DELETE请求
		del(w, r)
		return
	}
	// 如果请求方法不是上述四种之一，则返回405 Method Not Allowed
	w.WriteHeader(http.StatusMethodNotAllowed)
}
