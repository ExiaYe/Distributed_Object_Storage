package objectstream

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type TempPutStream struct {
	Server string
	Uuid   string
}

// 创建一个临时上传流
func NewTempPutStream(server, object string, size int64) (*TempPutStream, error) {
	// 创建一个POST请求
	request, e := http.NewRequest("POST", "http://"+server+"/temp/"+object, nil)
	if e != nil {
		return nil, e
	}
	// 设置请求头中的size字段
	request.Header.Set("size", fmt.Sprintf("%d", size))
	// 创建一个http客户端
	client := http.Client{}
	// 发送请求
	response, e := client.Do(request)
	if e != nil {
		return nil, e
	}
	// 读取响应体中的uuid
	uuid, e := ioutil.ReadAll(response.Body)
	if e != nil {
		return nil, e
	}
	// 返回一个临时上传流
	return &TempPutStream{server, string(uuid)}, nil
}

// 写入数据到临时上传流
func (w *TempPutStream) Write(p []byte) (n int, err error) {
	// 创建一个PATCH请求
	request, e := http.NewRequest("PATCH", "http://"+w.Server+"/temp/"+w.Uuid, strings.NewReader(string(p)))
	if e != nil {
		return 0, e
	}
	// 创建一个http客户端
	client := http.Client{}
	// 发送请求
	r, e := client.Do(request)
	if e != nil {
		return 0, e
	}
	// 如果响应状态码不是200，则返回错误
	if r.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("dataServer return http code %d", r.StatusCode)
	}
	// 返回写入的字节数
	return len(p), nil
}

// 提交临时上传流
func (w *TempPutStream) Commit(good bool) {
	// 设置请求方法
	method := "DELETE"
	if good {
		method = "PUT"
	}
	// 创建一个请求
	request, _ := http.NewRequest(method, "http://"+w.Server+"/temp/"+w.Uuid, nil)
	// 创建一个http客户端
	client := http.Client{}
	// 发送请求
	client.Do(request)
}

func NewTempGetStream(server, uuid string) (*GetStream, error) {
	return newGetStream("http://" + server + "/temp/" + uuid)
}
