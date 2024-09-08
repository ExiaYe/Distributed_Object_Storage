package heartbeat

import (
	"Distributed_Object_Storage/src/lib/rabbitmq"
	"strconv"
	"sync"
	"time"
)

// 接收和处理来自数据服务节点的心跳消息

// 缓存所有的数据节点 key为数据节点的心跳消息即监听地址,value 为时间
var dataServers = make(map[string]time.Time)
var mutex sync.Mutex

// 监听心跳
func ListenHeartbeat() {
	// 创建rabbitmq连接
	q := rabbitmq.New(rabbitmq.RABBITMQ_SERVER)
	// 关闭连接
	defer q.Close()
	// 绑定队列
	q.Bind("apiServers")
	// 消费队列
	c := q.Consume()
	// 清除10s没收到心跳消息的数据节点
	go removeExpiredDataServer()
	// 循环消费队列
	for msg := range c {
		// 每个节点的监听地址
		dataServer, e := strconv.Unquote(string(msg.Body))
		if e != nil {
			panic(e)
		}
		// 加锁
		mutex.Lock()
		// 更新数据节点
		dataServers[dataServer] = time.Now()
		// 解锁
		mutex.Unlock()
	}
}

// removeExpiredDataServer函数用于删除过期的数据服务器
func removeExpiredDataServer() {
	// 无限循环
	for {
		// 每隔5秒钟执行一次
		time.Sleep(5 * time.Second)
		// 加锁
		mutex.Lock()
		// 遍历dataServers
		for s, t := range dataServers {
			// 检查节点的最后一次心跳消息时间是否在10秒钟之前
			if t.Add(10 * time.Second).Before(time.Now()) {
				// 如果是，则删除该节点
				delete(dataServers, s)
			}
		}
		// 解锁
		mutex.Unlock()
	}
}

// 获取所有的数据节点监听地址

// 获取数据服务器列表
func GetDataServers() []string {
	// 加锁
	mutex.Lock()
	// 在函数结束时解锁
	defer mutex.Unlock()
	// 创建一个空字符串切片
	ds := make([]string, 0)
	// 遍历dataServers
	for s, _ := range dataServers {
		// 将dataServers中的键添加到ds中
		ds = append(ds, s)
	}
	// 返回ds
	return ds
}
