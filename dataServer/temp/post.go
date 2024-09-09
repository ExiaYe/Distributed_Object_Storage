package temp

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type tempInfo struct {
	Uuid string
	Name string
	Size int64
}

func post(w http.ResponseWriter, r *http.Request) {
	// uuidgen 是一个在 Unix/Linux 系统中可用的命令行工具，用于生成 UUID
	output, _ := exec.Command("uuidgen").Output()
	uuid := strings.TrimSuffix(string(output), "\n")
	// 从 URL 中获取文件名
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 从请求头中获取文件大小
	size, e := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if e != nil {
		log.Println(e)
		// 返回 500 错误
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 创建临时文件信息
	t := tempInfo{uuid, name, size}
	// 将临时文件信息写入文件
	e = t.writeToFile()
	if e != nil {
		log.Println(e)
		// 返回 500 错误
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 创建临时文件
	os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid + ".dat")
	// 返回生成的 UUID
	w.Write([]byte(uuid))
}

// writeToFile函数用于将tempInfo结构体写入文件
func (t *tempInfo) writeToFile() error {
	// 创建文件，文件名为STORAGE_ROOT环境变量下的/temp/目录下的Uuid文件
	f, e := os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid)
	// 如果创建文件失败，返回错误
	if e != nil {
		return e
	}
	// 关闭文件
	defer f.Close()
	// 将tempInfo结构体转换为json格式
	b, _ := json.Marshal(t)
	// 将json格式写入文件
	f.Write(b)
	// 返回nil表示成功
	return nil
}
