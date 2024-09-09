package temp

import (
	"log"
	"net/http"
	"os"
	"strings"
)

// 定义一个put函数，用于处理HTTP请求
func put(w http.ResponseWriter, r *http.Request) {
	// 从URL中获取uuid
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 从文件中读取uuid对应的信息
	tempinfo, e := readFromFile(uuid)
	if e != nil {
		// 如果读取失败，则打印错误信息，并返回404状态码
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 获取存储根目录
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	// 获取数据文件路径
	datFile := infoFile + ".dat"
	// 打开数据文件
	f, e := os.Open(datFile)
	if e != nil {
		// 如果打开失败，则打印错误信息，并返回500状态码
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 关闭文件
	defer f.Close()
	// 获取文件信息
	info, e := f.Stat()
	if e != nil {
		// 如果获取文件信息失败，则打印错误信息，并返回500状态码
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 获取文件大小
	actual := info.Size()
	// 删除临时文件
	os.Remove(infoFile)
	// 如果文件大小不匹配，则删除数据文件
	if actual != tempinfo.Size {
		os.Remove(datFile)
		// 打印错误信息
		log.Println("actual size mismatch, expect", tempinfo.Size, "actual", actual)
		// 返回500状态码
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 提交临时对象
	commitTempObject(datFile, tempinfo)
}
