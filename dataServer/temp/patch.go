package temp

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

// patch函数用于处理PATCH请求，将请求体中的数据追加到指定文件中
func patch(w http.ResponseWriter, r *http.Request) {
	// 从URL中获取uuid
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	// 从文件中读取tempinfo
	tempinfo, e := readFromFile(uuid)
	if e != nil {
		// 如果读取失败，则返回404状态码
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// 获取存储根目录
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	// 获取数据文件路径
	datFile := infoFile + ".dat"
	// 打开数据文件，以只写和追加模式打开
	f, e := os.OpenFile(datFile, os.O_WRONLY|os.O_APPEND, 0)
	if e != nil {
		// 如果打开文件失败，则返回500状态码
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 关闭文件
	defer f.Close()
	// 将请求体中的数据追加到文件中
	_, e = io.Copy(f, r.Body)
	if e != nil {
		// 如果追加数据失败，则返回500状态码
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 获取文件状态
	info, e := f.Stat()
	if e != nil {
		// 如果获取文件状态失败，则返回500状态码
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 获取文件大小
	actual := info.Size()
	// 如果文件大小超过tempinfo中的大小，则删除文件
	if actual > tempinfo.Size {
		os.Remove(datFile)
		os.Remove(infoFile)
		log.Println("actual size", actual, "exceeds", tempinfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// 从文件中读取数据
func readFromFile(uuid string) (*tempInfo, error) {
	// 打开文件
	f, e := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid)
	// 如果打开文件出错，返回错误
	if e != nil {
		return nil, e
	}
	// 关闭文件
	defer f.Close()
	// 读取文件内容
	b, _ := ioutil.ReadAll(f)
	// 定义一个tempInfo类型的变量
	var info tempInfo
	// 将文件内容解析为tempInfo类型的变量
	json.Unmarshal(b, &info)
	// 返回解析后的变量
	return &info, nil
}
