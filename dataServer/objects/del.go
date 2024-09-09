package objects

import (
	"Distributed_Object_Storage/dataServer/locate"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// 删除文件
func del(w http.ResponseWriter, r *http.Request) {
	// 获取URL中的hash值
	hash := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 获取文件路径
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/" + hash + ".*")
	// 如果文件不存在，则返回
	if len(files) != 1 {
		return
	}
	// 删除文件
	locate.Del(hash)
	// 将文件移动到垃圾箱
	os.Rename(files[0], os.Getenv("STORAGE_ROOT")+"/garbage/"+filepath.Base(files[0]))
}
