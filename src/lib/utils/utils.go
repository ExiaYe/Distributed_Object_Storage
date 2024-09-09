package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// 从http.Header中获取偏移量
func GetOffsetFromHeader(h http.Header) int64 {
	// 获取range字段
	byteRange := h.Get("range")
	// 如果range字段长度小于7，则返回0
	if len(byteRange) < 7 {
		return 0
	}
	// 如果range字段前6个字符不是"bytes="，则返回0
	if byteRange[:6] != "bytes=" {
		return 0
	}
	// 将range字段从"-"分割成两个部分
	bytePos := strings.Split(byteRange[6:], "-")
	// 将第一个部分转换为int64类型
	offset, _ := strconv.ParseInt(bytePos[0], 0, 64)
	// 返回偏移量
	return offset
}

// 从http.Header中获取哈希值
func GetHashFromHeader(h http.Header) string {
	// 获取digest字段
	digest := h.Get("digest")
	// 如果digest字段长度小于9，则返回空字符串
	if len(digest) < 9 {
		return ""
	}
	// 如果digest字段前8个字符不是"SHA-256="，则返回空字符串
	if digest[:8] != "SHA-256=" {
		return ""
	}
	// 返回digest字段从第9个字符开始的部分
	return digest[8:]
}

// 从http.Header中获取文件大小
func GetSizeFromHeader(h http.Header) int64 {
	// 获取content-length字段
	size, _ := strconv.ParseInt(h.Get("content-length"), 0, 64)
	// 返回文件大小
	return size
}

// 计算文件的哈希值
func CalculateHash(r io.Reader) string {
	// 创建sha256哈希对象
	h := sha256.New()
	// 将文件内容复制到哈希对象中
	io.Copy(h, r)
	// 将哈希对象转换为base64编码的字符串
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
