package objects

import (
	"Distributed_Object_Storage/apiServer/locate"
	"Distributed_Object_Storage/src/lib/utils"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// storeObject函数用于将对象存储到服务器上
func storeObject(r io.Reader, hash string, size int64) (int, error) {
	// 如果对象已经存在，则返回200状态码
	if locate.Exist(url.PathEscape(hash)) {
		return http.StatusOK, nil
	}

	// 创建一个流，用于将对象存储到服务器上
	stream, e := putStream(url.PathEscape(hash), size)
	if e != nil {
		// 如果创建流失败，则返回500状态码和错误信息
		return http.StatusInternalServerError, e
	}

	// 创建一个TeeReader，用于同时读取对象和计算对象的哈希值
	reader := io.TeeReader(r, stream)
	// 计算对象的哈希值
	d := utils.CalculateHash(reader)
	// 如果计算出的哈希值与请求的哈希值不匹配，则返回400状态码和错误信息
	if d != hash {
		stream.Commit(false)
		return http.StatusBadRequest, fmt.Errorf("object hash mismatch, calculated=%s, requested=%s", d, hash)
	}
	// 提交流，将对象存储到服务器上
	stream.Commit(true)
	// 返回200状态码
	return http.StatusOK, nil
}
