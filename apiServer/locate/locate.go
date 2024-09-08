package locate

import (
	"Distributed_Object_Storage/src/lib/rabbitmq"
	"Distributed_Object_Storage/src/lib/rs"
	"Distributed_Object_Storage/src/lib/types"
	"encoding/json"
	"os"
	"time"
)

// Locate函数用于定位数据服务器
func Locate(name string) (locateInfo map[int]string) {
	// 创建一个rabbitmq实例
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	// 发布消息到dataServers队列
	q.Publish("dataServers", name)
	// 消费dataServers队列的消息
	c := q.Consume()
	// 启动一个goroutine，等待1秒后关闭rabbitmq实例
	go func() {
		time.Sleep(time.Second)
		q.Close()
	}()
	// 创建一个map用于存储定位信息
	locateInfo = make(map[int]string)
	// 遍历所有分片
	for i := 0; i < rs.ALL_SHARDS; i++ {
		// 从队列中获取消息
		msg := <-c
		// 如果消息为空，则返回
		if len(msg.Body) == 0 {
			return
		}
		// 解析消息
		var info types.LocateMessage
		json.Unmarshal(msg.Body, &info)
		// 将定位信息存储到map中
		locateInfo[info.Id] = info.Addr
	}
	// 返回定位信息
	return
}

func Exist(name string) bool {
	return len(Locate(name)) >= rs.DATA_SHARDS
}
