package main

import (
	"Distributed_Object_Storage/apiServer/objects"
	"Distributed_Object_Storage/src/lib/es"
	"Distributed_Object_Storage/src/lib/utils"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")

	for i := range files {
		hash := strings.Split(filepath.Base(files[i]), ".")[0]
		verify(hash)
	}
}

// 验证哈希
func verify(hash string) {
	// 打印验证哈希
	log.Println("verify", hash)
	// 获取哈希大小
	size, e := es.SearchHashSize(hash)
	// 如果获取哈希大小失败，打印错误信息
	if e != nil {
		log.Println(e)
		return
	}
	// 获取哈希流
	stream, e := objects.GetStream(hash, size)
	// 如果获取哈希流失败，打印错误信息
	if e != nil {
		log.Println(e)
		return
	}
	// 计算哈希
	d := utils.CalculateHash(stream)
	// 如果计算出的哈希与请求的哈希不匹配，打印错误信息
	if d != hash {
		log.Printf("object hash mismatch, calculated=%s, requested=%s", d, hash)
	}
	// 关闭哈希流
	stream.Close()
}
