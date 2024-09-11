package objectstream

import (
	"fmt"
	"io"
	"net/http"
)

type PutStream struct {
	writer *io.PipeWriter
	c      chan error
}

// 创建一个新的PutStream，用于向指定的服务器上传对象
func NewPutStream(server, object string) *PutStream {
	// 创建一个管道，用于读取和写入数据
	reader, writer := io.Pipe()
	// 创建一个通道，用于接收错误信息
	c := make(chan error)
	// 启动一个goroutine，用于发送HTTP请求
	go func() {
		// 创建一个HTTP请求，请求方法为PUT，请求地址为http://server/objects/object
		request, _ := http.NewRequest("PUT", "http://"+server+"/objects/"+object, reader)
		// 创建一个HTTP客户端
		client := http.Client{}
		// 发送HTTP请求
		r, e := client.Do(request)
		// 如果请求成功，但返回的状态码不是200，则返回错误信息
		if e == nil && r.StatusCode != http.StatusOK {
			e = fmt.Errorf("dataServer return http code %d", r.StatusCode)
		}
		// 将错误信息发送到通道
		c <- e
	}()
	// 返回一个PutStream，包含写入器和错误通道
	return &PutStream{writer, c}
}

func (w *PutStream) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *PutStream) Close() error {
	w.writer.Close()
	return <-w.c
}
