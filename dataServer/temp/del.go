package temp

import (
	"net/http"
	"os"
	"strings"
)

// 删除文件
func del(w http.ResponseWriter, r *http.Request) {
	// 获取URL中的UUID
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 获取存储根目录
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	// 获取数据文件路径
	datFile := infoFile + ".dat"
	// 删除info文件
	os.Remove(infoFile)
	// 删除dat文件
	os.Remove(datFile)
}
