package objects

import (
	"Distributed_Object_Storage/apiServer/heartbeat"
	"Distributed_Object_Storage/src/lib/rs"
	"fmt"
)

// 根据给定的hash和size，创建一个新的RSPutStream
func putStream(hash string, size int64) (*rs.RSPutStream, error) {
	// 从心跳模块中选择随机的数据服务器
	servers := heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS, nil)
	// 如果选择的数据服务器数量不等于总的数据服务器数量，则返回错误
	if len(servers) != rs.ALL_SHARDS {
		return nil, fmt.Errorf("cannot find enough dataServer")
	}

	// 创建一个新的RSPutStream
	return rs.NewRSPutStream(servers, hash, size)
}
