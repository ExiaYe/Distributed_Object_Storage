package heartbeat

import (
	"math/rand"
)

// 随机选择一个数据节点

// ChooseRandomDataServers 函数用于从给定的数据服务器列表中随机选择n个服务器，并排除掉exclude列表中的服务器
func ChooseRandomDataServers(n int, exclude map[int]string) (ds []string) {
	// 创建一个空的候选服务器列表
	candidates := make([]string, 0)
	// 创建一个反向的exclude映射，用于快速查找
	reverseExcludeMap := make(map[string]int)
	// 遍历exclude映射，将地址和id存入reverseExcludeMap
	for id, addr := range exclude {
		reverseExcludeMap[addr] = id
	}
	// 获取所有数据服务器
	servers := GetDataServers()
	// 遍历所有数据服务器
	for i := range servers {
		s := servers[i]
		// 检查当前服务器是否在exclude列表中
		_, excluded := reverseExcludeMap[s]
		// 如果不在exclude列表中，则将其加入候选服务器列表
		if !excluded {
			candidates = append(candidates, s)
		}
	}
	// 获取候选服务器的数量
	length := len(candidates)
	// 如果候选服务器的数量小于n，则返回空列表
	if length < n {
		return
	}
	// 生成一个随机排列
	p := rand.Perm(length)
	// 遍历随机排列，将对应的候选服务器加入结果列表
	for i := 0; i < n; i++ {
		ds = append(ds, candidates[p[i]])
	}
	// 返回结果列表
	return
}
