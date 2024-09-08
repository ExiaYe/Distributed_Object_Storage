package objects

import (
	"Distributed_Object_Storage/apiServer/heartbeat"
	"Distributed_Object_Storage/apiServer/locate"
	"Distributed_Object_Storage/src/lib/es"
	"Distributed_Object_Storage/src/lib/rs"
	"Distributed_Object_Storage/src/lib/utils"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// 处理POST请求
func post(w http.ResponseWriter, r *http.Request) {
	// 从URL中获取文件名
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 从请求头中获取文件大小
	size, e := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	// 如果获取文件大小失败，则返回403 Forbidden
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// 从请求头中获取文件哈希值
	hash := utils.GetHashFromHeader(r.Header)
	// 如果没有获取到文件哈希值，则返回400 Bad Request
	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// 如果文件已经存在，则添加版本号
	if locate.Exist(url.PathEscape(hash)) {
		e = es.AddVersion(name, hash, size)
		// 如果添加版本号失败，则返回500 Internal Server Error
		if e != nil {
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			// 如果添加版本号成功，则返回200 OK
			w.WriteHeader(http.StatusOK)
		}
		return
	}
	// 随机选择数据服务器
	ds := heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS, nil)
	// 如果没有选择到足够的数据服务器，则返回503 Service Unavailable
	if len(ds) != rs.ALL_SHARDS {
		log.Println("cannot find enough dataServer")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	// 创建可恢复的文件上传流
	stream, e := rs.NewRSResumablePutStream(ds, name, url.PathEscape(hash), size)
	// 如果创建可恢复的文件上传流失败，则返回500 Internal Server Error
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 设置location头，指向临时文件
	w.Header().Set("location", "/temp/"+url.PathEscape(stream.ToToken()))
	// 返回201 Created
	w.WriteHeader(http.StatusCreated)
}
