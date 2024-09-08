package objects

import (
	"Distributed_Object_Storage/apiServer/heartbeat"
	"Distributed_Object_Storage/apiServer/locate"
	"Distributed_Object_Storage/src/lib/rs"
	"fmt"
)

// 根据哈希值和大小获取流
func GetStream(hash string, size int64) (*rs.RSGetStream, error) {
	// 根据哈希值获取定位信息
	locateInfo := locate.Locate(hash)
	// 如果定位信息中的数据分片数量小于数据分片数量，则返回错误
	if len(locateInfo) < rs.DATA_SHARDS {
		return nil, fmt.Errorf("object %s locate fail, result %v", hash, locateInfo)
	}
	// 创建一个空的数据服务器切片
	dataServers := make([]string, 0)
	// 如果定位信息中的分片数量不等于所有分片数量，则随机选择数据服务器
	if len(locateInfo) != rs.ALL_SHARDS {
		dataServers = heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS-len(locateInfo), locateInfo)
	}
	// 返回一个新的RSGetStream
	return rs.NewRSGetStream(locateInfo, dataServers, hash, size)
}
