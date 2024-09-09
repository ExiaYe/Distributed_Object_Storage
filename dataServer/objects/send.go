package objects

import (
	"compress/gzip"
	"io"
	"log"
	"os"
)

// 函数sendFile用于将文件发送到指定的io.Writer中
func sendFile(w io.Writer, file string) {
	// 打开文件
	f, e := os.Open(file)
	// 如果打开文件出错，则打印错误信息并返回
	if e != nil {
		log.Println(e)
		return
	}
	// 关闭文件
	defer f.Close()
	// 创建gzip压缩流
	gzipStream, e := gzip.NewReader(f)
	// 如果创建gzip压缩流出错，则打印错误信息并返回
	if e != nil {
		log.Println(e)
		return
	}
	// 将gzip压缩流中的数据拷贝到指定的io.Writer中
	io.Copy(w, gzipStream)
	// 关闭gzip压缩流
	gzipStream.Close()
}
