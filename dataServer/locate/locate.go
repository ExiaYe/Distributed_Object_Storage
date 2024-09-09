package locate

import (
	"Distributed_Object_Storage/src/lib/rabbitmq"
	"Distributed_Object_Storage/src/lib/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// map的 key存放 对象的散列值，value 存放分片id
var objects = make(map[string]int)
var mutex sync.Mutex

func Locate(hash string) int {
	mutex.Lock()
	id, ok := objects[hash]
	mutex.Unlock()
	if !ok {
		return -1
	}
	return id
}

func Add(hash string, id int) {
	mutex.Lock()
	objects[hash] = id
	mutex.Unlock()
}

func Del(hash string) {
	mutex.Lock()
	delete(objects, hash)
	mutex.Unlock()
}

// StartLocate函数用于启动定位服务
func StartLocate() {
	// 创建一个新的rabbitmq实例
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	// 程序结束时关闭rabbitmq实例
	defer q.Close()
	// 绑定到名为"dataServers"的队列
	q.Bind("dataServers")
	// 消费队列中的消息
	c := q.Consume()
	// 遍历消费到的消息
	for msg := range c {
		// 将消息体转换为字符串
		hash, e := strconv.Unquote(string(msg.Body))
		// 如果转换失败，则抛出异常
		if e != nil {
			panic(e)
		}
		// 调用Locate函数，根据hash值定位数据
		id := Locate(hash)
		// 如果定位成功，则发送定位消息
		if id != -1 {
			q.Send(msg.ReplyTo, types.LocateMessage{Addr: os.Getenv("LISTEN_ADDRESS"), Id: id})
		}
	}
}

// CollectObjects函数用于收集存储根目录下的所有对象
func CollectObjects() {
	// 获取存储根目录下的所有对象文件
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	// 遍历所有对象文件
	for i := range files {
		// 获取文件名
		file := strings.Split(filepath.Base(files[i]), ".")
		// 如果文件名不符合要求，则抛出异常
		if len(file) != 3 {
			panic(files[i])
		}
		// 获取文件名中的hash值
		hash := file[0]
		// 将文件名中的id值转换为整数
		id, e := strconv.Atoi(file[1])
		// 如果转换失败，则抛出异常
		if e != nil {
			panic(e)
		}
		// 将hash值和id值存入objects字典中
		objects[hash] = id
	}
}
