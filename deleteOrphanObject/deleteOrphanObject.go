package main

import (
	"Distributed_Object_Storage/src/lib/es"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	// 获取环境变量STORAGE_ROOT的值，并拼接成文件路径
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")

	// 遍历文件列表
	for i := range files {
		// 获取文件名，并去掉后缀
		hash := strings.Split(filepath.Base(files[i]), ".")[0]
		// 查询文件哈希是否在元数据中
		hashInMetadata, e := es.HasHash(hash)
		// 如果查询出错，打印错误信息并返回
		if e != nil {
			log.Println(e)
			return
		}
		// 如果文件哈希不在元数据中，则删除文件
		if !hashInMetadata {
			del(hash)
		}
	}
}

// 删除指定hash的对象
func del(hash string) {
	// 打印删除的hash
	log.Println("delete", hash)
	// 构造删除对象的url
	url := "http://" + os.Getenv("LISTEN_ADDRESS") + "/objects/" + hash
	// 创建DELETE请求
	request, _ := http.NewRequest("DELETE", url, nil)
	// 创建http客户端
	client := http.Client{}
	// 发送请求
	client.Do(request)
}
