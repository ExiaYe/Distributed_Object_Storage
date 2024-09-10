package es

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Metadata struct {
	Name    string
	Version int
	Size    int64
	Hash    string
}

type hit struct {
	Source Metadata `json:"_source"`
}

type searchResult struct {
	Hits struct {
		Total int
		Hits  []hit
	}
}

// 根据对象名称和版本号获取元数据
func getMetadata(name string, versionId int) (meta Metadata, e error) {
	// 构造请求URL
	url := fmt.Sprintf("http://%s/metadata/objects/%s_%d/_source",
		os.Getenv("ES_SERVER"), name, versionId)
	// 发送GET请求
	r, e := http.Get(url)
	if e != nil {
		return
	}
	// 如果返回状态码不是200，则返回错误
	if r.StatusCode != http.StatusOK {
		e = fmt.Errorf("fail to get %s_%d: %d", name, versionId, r.StatusCode)
		return
	}
	// 读取响应体
	result, _ := ioutil.ReadAll(r.Body)
	// 解析JSON数据
	json.Unmarshal(result, &meta)
	return
}

// 根据对象名称搜索最新版本
func SearchLatestVersion(name string) (meta Metadata, e error) {
	// 构造请求URL
	url := fmt.Sprintf("http://%s/metadata/_search?q=name:%s&size=1&sort=version:desc",
		os.Getenv("ES_SERVER"), url.PathEscape(name))
	// 发送GET请求
	r, e := http.Get(url)
	if e != nil {
		return
	}
	// 如果返回状态码不是200，则返回错误
	if r.StatusCode != http.StatusOK {
		e = fmt.Errorf("fail to search latest metadata: %d", r.StatusCode)
		return
	}
	// 读取响应体
	result, _ := ioutil.ReadAll(r.Body)
	// 解析JSON数据
	var sr searchResult
	json.Unmarshal(result, &sr)
	// 如果搜索结果不为空，则返回最新版本
	if len(sr.Hits.Hits) != 0 {
		meta = sr.Hits.Hits[0].Source
	}
	return
}

// 根据对象名称和版本号获取元数据，如果版本号为0，则搜索最新版本
func GetMetadata(name string, version int) (Metadata, error) {
	if version == 0 {
		return SearchLatestVersion(name)
	}
	return getMetadata(name, version)
}

// 将元数据添加到ES中
func PutMetadata(name string, version int, size int64, hash string) error {
	// 构造JSON数据
	doc := fmt.Sprintf(`{"name":"%s","version":%d,"size":%d,"hash":"%s"}`,
		name, version, size, hash)
	// 创建HTTP客户端
	client := http.Client{}
	// 构造请求URL
	url := fmt.Sprintf("http://%s/metadata/objects/%s_%d?op_type=create",
		os.Getenv("ES_SERVER"), name, version)
	// 创建PUT请求
	request, _ := http.NewRequest("PUT", url, strings.NewReader(doc))
	// 发送请求
	r, e := client.Do(request)
	if e != nil {
		return e
	}
	// 如果返回状态码是409，则递归调用PutMetadata函数，版本号加1
	if r.StatusCode == http.StatusConflict {
		return PutMetadata(name, version+1, size, hash)
	}
	// 如果返回状态码不是201，则返回错误
	if r.StatusCode != http.StatusCreated {
		result, _ := ioutil.ReadAll(r.Body)
		return fmt.Errorf("fail to put metadata: %d %s", r.StatusCode, string(result))
	}
	return nil
}

// 添加版本
func AddVersion(name, hash string, size int64) error {
	// 搜索最新版本
	version, e := SearchLatestVersion(name)
	if e != nil {
		return e
	}
	// 将新版本添加到ES中
	return PutMetadata(name, version.Version+1, size, hash)
}

// 搜索所有版本
func SearchAllVersions(name string, from, size int) ([]Metadata, error) {
	// 构造请求URL
	url := fmt.Sprintf("http://%s/metadata/_search?sort=name,version&from=%d&size=%d",
		os.Getenv("ES_SERVER"), from, size)
	// 如果对象名称不为空，则添加到URL中
	if name != "" {
		url += "&q=name:" + name
	}
	// 发送GET请求
	r, e := http.Get(url)
	if e != nil {
		return nil, e
	}
	// 创建元数据切片
	metas := make([]Metadata, 0)
	// 读取响应体
	result, _ := ioutil.ReadAll(r.Body)
	// 解析JSON数据
	var sr searchResult
	json.Unmarshal(result, &sr)
	// 将搜索结果添加到元数据切片中
	for i := range sr.Hits.Hits {
		metas = append(metas, sr.Hits.Hits[i].Source)
	}
	return metas, nil
}

// 删除元数据
func DelMetadata(name string, version int) {
	// 创建HTTP客户端
	client := http.Client{}
	// 构造请求URL
	url := fmt.Sprintf("http://%s/metadata/objects/%s_%d",
		os.Getenv("ES_SERVER"), name, version)
	// 创建DELETE请求
	request, _ := http.NewRequest("DELETE", url, nil)
	// 发送请求
	client.Do(request)
}

type Bucket struct {
	Key         string
	Doc_count   int
	Min_version struct {
		Value float32
	}
}

type aggregateResult struct {
	Aggregations struct {
		Group_by_name struct {
			Buckets []Bucket
		}
	}
}

// SearchVersionStatus 函数用于搜索指定文档数量的版本状态
func SearchVersionStatus(min_doc_count int) ([]Bucket, error) {
	// 创建一个http客户端
	client := http.Client{}
	// 获取ES服务器的地址
	url := fmt.Sprintf("http://%s/metadata/_search", os.Getenv("ES_SERVER"))
	// 构造请求体
	body := fmt.Sprintf(`
        {
          "size": 0,
          "aggs": {
            "group_by_name": {
              "terms": {
                "field": "name",
                "min_doc_count": %d
              },
              "aggs": {
                "min_version": {
                  "min": {
                    "field": "version"
                  }
                }
              }
            }
          }
        }`, min_doc_count)
	// 创建一个GET请求
	request, _ := http.NewRequest("GET", url, strings.NewReader(body))
	// 发送请求
	r, e := client.Do(request)
	if e != nil {
		return nil, e
	}
	// 读取响应体
	b, _ := ioutil.ReadAll(r.Body)
	// 解析响应体
	var ar aggregateResult
	json.Unmarshal(b, &ar)
	// 返回聚合结果
	return ar.Aggregations.Group_by_name.Buckets, nil
}

// HasHash 函数用于判断指定hash是否存在
func HasHash(hash string) (bool, error) {
	// 获取ES服务器的地址
	url := fmt.Sprintf("http://%s/metadata/_search?q=hash:%s&size=0", os.Getenv("ES_SERVER"), hash)
	// 发送GET请求
	r, e := http.Get(url)
	if e != nil {
		return false, e
	}
	// 读取响应体
	b, _ := ioutil.ReadAll(r.Body)
	// 解析响应体
	var sr searchResult
	json.Unmarshal(b, &sr)
	// 返回是否存在
	return sr.Hits.Total != 0, nil
}

// SearchHashSize 函数用于搜索指定hash的大小
func SearchHashSize(hash string) (size int64, e error) {
	// 获取ES服务器的地址
	url := fmt.Sprintf("http://%s/metadata/_search?q=hash:%s&size=1",
		os.Getenv("ES_SERVER"), hash)
	// 发送GET请求
	r, e := http.Get(url)
	if e != nil {
		return
	}
	// 判断响应状态码
	if r.StatusCode != http.StatusOK {
		e = fmt.Errorf("fail to search hash size: %d", r.StatusCode)
		return
	}
	// 读取响应体
	result, _ := ioutil.ReadAll(r.Body)
	// 解析响应体
	var sr searchResult
	json.Unmarshal(result, &sr)
	// 返回大小
	if len(sr.Hits.Hits) != 0 {
		size = sr.Hits.Hits[0].Source.Size
	}
	return
}
