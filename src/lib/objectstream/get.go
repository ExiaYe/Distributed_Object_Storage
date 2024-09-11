package objectstream

import (
	"fmt"
	"io"
	"net/http"
)

type GetStream struct {
	reader io.Reader
}

// newGetStream函数用于获取指定URL的数据流
func newGetStream(url string) (*GetStream, error) {
	// 发送HTTP GET请求
	r, e := http.Get(url)
	// 如果请求出错，返回错误
	if e != nil {
		return nil, e
	}
	// 如果HTTP状态码不是200，返回错误
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dataServer return http code %d", r.StatusCode)
	}
	// 返回数据流
	return &GetStream{r.Body}, nil
}

// NewGetStream函数用于获取指定服务器和对象的URL，并调用newGetStream函数获取数据流
func NewGetStream(server, object string) (*GetStream, error) {
	// 如果服务器或对象为空，返回错误
	if server == "" || object == "" {
		return nil, fmt.Errorf("invalid server %s object %s", server, object)
	}
	// 调用newGetStream函数获取数据流
	return newGetStream("http://" + server + "/objects/" + object)
}

func (r *GetStream) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}
