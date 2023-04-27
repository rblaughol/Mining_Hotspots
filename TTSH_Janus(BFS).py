from gremlin_python.driver import client
from gremlin_python.structure.graph import Graph
from os import sys
local_client = client.Client('ws://127.0.0.1:8182/gremlin', 'g')
# 定义频繁度阈值和距离阈值
step = 3
min_pin = 2
# 定义当前的距离阈值，最短为2
current_step = 2
hotspot_list = []
# 当前的轨迹热点
current_hotspot = []
if __name__ == '__main__':
	# 查询边
	cypher = "g.V().outE()"
	edge_list = local_client.submit(cypher)
	# 遍历边
	for edge_small_list in edge_list:
		for edge in edge_small_list:
			# 提取度
			re_id = edge.id['@value']['relationId']
			cypher = "g.E('" + re_id + "').values()"
			values = local_client.submit(cypher)
			value = next(iter(values))[0]
			# 判断是否满足频繁度阈值
			if value >= min_pin:
				current_hotspot.append([edge])

	# 生成轨迹热点
	while(len(current_hotspot[0]) < step):
		tmp_hotspot = []
		# 遍历当前热点区域
		for e in current_hotspot:
			# 提取图数据库中的id
			re_id = e[-1].id['@value']['relationId']
			try:
				# 查询相邻的边
				cypher = "g.E('" + re_id + "').inV().outE()"
				edge_list = local_client.submit(cypher)
				for edge_small_list in edge_list:
					# 遍历相邻的边
					for edge in edge_small_list:
						# 提取id
						re_id = edge.id['@value']['relationId']
						# 查询度
						cypher = "g.E('" + re_id + "').values()"
						values = local_client.submit(cypher)
						value = next(iter(values))[0]
						# 判断是否满足中心性算法
						if value >= min_pin:
							tmp_hotspot.append(e+[edge])

			except:
				pass
		# 没有找到轨迹热点
		if tmp_hotspot == []:
			hotspot_list = current_hotspot
			print("no hotspots, longest is:", current_hotspot)
			sys.exit()

		current_hotspot = tmp_hotspot

	hotspot_list = current_hotspot
	print("hotspots:", hotspot_list)

