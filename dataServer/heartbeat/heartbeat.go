package heartbeat

import (
	"Distributed_Object_Storage/src/lib/rabbitmq"
	"os"
	"time"
)

// 启动心跳
func StartHeartbeat() {
	// 创建rabbitmq连接
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	// 关闭连接
	defer q.Close()
	// 无限循环
	for {
		// 发布消息
		q.Publish("apiServers", os.Getenv("LISTEN_ADDRESS"))
		// 休眠5秒
		time.Sleep(5 * time.Second)
	}
}
