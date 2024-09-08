package objects

import (
	"Distributed_Object_Storage/src/lib/es"
	"Distributed_Object_Storage/src/lib/utils"
	"log"
	"net/http"
	"strings"
)

// 处理PUT请求
func put(w http.ResponseWriter, r *http.Request) {
	// 从请求头中获取对象的哈希值
	hash := utils.GetHashFromHeader(r.Header)
	// 如果哈希值为空，则返回错误
	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 从请求头中获取对象的大小
	size := utils.GetSizeFromHeader(r.Header)
	// 将对象存储到存储系统中
	c, e := storeObject(r.Body, hash, size)
	// 如果存储过程中出现错误，则返回错误
	if e != nil {
		log.Println(e)
		w.WriteHeader(c)
		return
	}
	// 如果存储过程中返回的状态码不是200，则返回错误
	if c != http.StatusOK {
		w.WriteHeader(c)
		return
	}

	// 从URL中获取对象的名称
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 将对象的版本信息添加到Elasticsearch中
	e = es.AddVersion(name, hash, size)
	// 如果添加过程中出现错误，则返回错误
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
