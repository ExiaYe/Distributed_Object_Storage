package temp

import (
	"Distributed_Object_Storage/dataServer/locate"
	"Distributed_Object_Storage/src/lib/utils"
	"compress/gzip"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func (t *tempInfo) hash() string {
	s := strings.Split(t.Name, ".")
	return s[0]
}

func (t *tempInfo) id() int {
	s := strings.Split(t.Name, ".")
	id, _ := strconv.Atoi(s[1])
	return id
}

// 函数commitTempObject用于将临时对象提交到存储系统中
func commitTempObject(datFile string, tempinfo *tempInfo) {
	// 打开datFile文件
	f, _ := os.Open(datFile)
	// 关闭文件
	defer f.Close()
	// 计算文件的哈希值
	d := url.PathEscape(utils.CalculateHash(f))
	// 将文件指针移动到文件开头
	f.Seek(0, io.SeekStart)
	// 创建存储对象文件
	w, _ := os.Create(os.Getenv("STORAGE_ROOT") + "/objects/" + tempinfo.Name + "." + d)
	// 创建gzip压缩器
	w2 := gzip.NewWriter(w)
	// 将datFile文件内容复制到存储对象文件中
	io.Copy(w2, f)
	// 关闭gzip压缩器
	w2.Close()
	// 删除datFile文件
	os.Remove(datFile)
	// 将对象哈希值和对象ID添加到locate中
	locate.Add(tempinfo.hash(), tempinfo.id())
}
