package objects

import (
	"Distributed_Object_Storage/src/lib/es"
	"log"
	"net/http"
	"strings"
)

// 删除函数
func del(w http.ResponseWriter, r *http.Request) {
	// 获取URL中的name参数
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 查询最新版本
	version, e := es.SearchLatestVersion(name)
	// 如果查询失败，则返回500错误
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 更新版本号
	e = es.PutMetadata(name, version.Version+1, 0, "")
	// 如果更新失败，则返回500错误
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
