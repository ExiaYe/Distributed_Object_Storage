package objects

import (
	"Distributed_Object_Storage/dataServer/locate"
	"crypto/sha256"
	"encoding/base64"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// 获取文件
func getFile(name string) string {
	// 获取文件路径
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/" + name + ".*")
	// 如果文件不存在，返回空字符串
	if len(files) != 1 {
		return ""
	}
	// 获取文件名
	file := files[0]
	// 创建sha256哈希对象
	h := sha256.New()
	// 发送文件
	sendFile(h, file)
	// 将哈希值转换为base64编码
	d := url.PathEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	// 获取文件哈希值
	hash := strings.Split(file, ".")[2]
	// 如果哈希值不匹配，删除文件
	if d != hash {
		log.Println("object hash mismatch, remove", file)
		locate.Del(hash)
		os.Remove(file)
		return ""
	}
	// 返回文件名
	return file
}
