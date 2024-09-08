package versions

import (
	"Distributed_Object_Storage/src/lib/es"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	// 获取请求的方法
	m := r.Method
	// 如果请求的方法不是GET，则返回405 Method Not Allowed
	if m != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// 初始化起始位置和大小
	from := 0
	size := 1000
	// 获取请求的路径中的第三个参数，即name
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 循环查找所有的版本
	for {
		// 查找所有的版本
		metas, e := es.SearchAllVersions(name, from, size)
		// 如果查找失败，则返回500 Internal Server Error
		if e != nil {
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// 遍历结果，将每一项结果通过json写入到响应中
		for i := range metas {
			// 每一项结果 通过json写入到响应中
			b, _ := json.Marshal(metas[i])
			w.Write(b)
			w.Write([]byte("\n"))
		}
		// 如果结果数量不等于size，则返回
		if len(metas) != size {
			return
		}
		// 起始位置增加size
		from += size
	}
}
