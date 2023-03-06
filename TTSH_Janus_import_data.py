from gremlin_python.driver import client
from gremlin_python.structure.graph import Graph
import csv
file_dir = "./tem/reduce_data.csv"
def init():
	csvfile = "./data_part.csv"
	"""
	第一次初始化，找出需要的列数据，对格式进行整理
	"""
	f = open(csvfile, 'r')  # 打开这个csv 存在在变量f中  r read 读  w write 写   b 以二进制形式打开
	# csv.writeheader
	with f:
		reader = csv.reader(f)  # 读取其中内容 放到 reader中
		path = "./part_data.csv"
		# 打开文件
		with open(path, 'w', newline="") as fd:
			writer = csv.writer(fd)
			# 轨迹的id（写入文件中的轨迹id）
			tra_id = 0
			# 前一组数据的人员和人员轨迹的id
			last_peo_id = 0
			last_peo_tra_id = 0
			# 读取数据的每一行
			for row in reader:  # FOR I IN RANGE(10):
				# 前两个是人员的id和人员轨迹的id
				peo_id = row[0]
				peo_tra_id = row[1]
				# 判断前一组数据的id是否一致
				if(peo_id == last_peo_id and peo_tra_id == last_peo_tra_id):
					# 如果一致那就正常运行
					num = tra_id
					last_peo_tra_id = peo_tra_id
					last_peo_id = peo_id
				else:
					# 如果不一致那就是下一条轨迹
					tra_id = tra_id + 1
					num = tra_id
					last_peo_tra_id = peo_tra_id
					last_peo_id = peo_id
				# 获取经纬度
				x1 = row[2:3]
				y1 = row[3]
				# 忽略第一行
				if x1 == ['x']:
					continue
				else:
					# 整理经度的格式
					x1 = x1[0]
					x1 = float(x1)
					x1 = str(x1)
					x1 = x1[0:7]
					# 整理纬度的格式
					y1 = float(y1)
					y1 = str(y1)
					y1 = y1[0:8]
					# 写入文件
					writer.writerow([num, x1, y1])  # 百度到用.writerow([1,2,3])
			fd.close()
		f.close()

	"""第二次数据清洗，去重"""
	# reduce文件清空
	with open("./tem/reduce_data.csv", 'r+') as file:
		file.truncate(0)
	file.close()
	# 打开上一个只有序号经纬度的文件
	f1 = open("./tem/part_data.csv", "r")
	with f1:
		reader1 = csv.reader(f1)
		# 读取这个文件
		for row in reader1:
			# 标志位，判断一列是否写入
			flags = 1
			f2 = open("./tem/reduce_data.csv", "r")
			with f2:
				# 读取已经写入的文件
				reader2 = csv.reader(f2)
				for rows in reader2:
					# 如果存在重复的就不再继续写入
					if row == rows:
						flags = 0
						break
				if flags == 1:
					f2.close()
					# 不存在重复的进行写入
					f3 = open("./tem/reduce_data.csv", "a", newline="")
					with f3:
						writer = csv.writer(f3)
						writer.writerow(row)
						f3.close()
		f1.close()
if __name__ == "__main__":
	# 连接janusgraph数据库
	init()
	local_client = client.Client('ws://127.0.0.1:8182/gremlin', 'g')
	cypher = "g"
	# print(cypher)
	with open(file_dir) as f:
		reader = csv.reader(f)
		# 第一个与其他的不一样
		# INDEX标识节点
		index = 1
		last_tr_id = '1'
		# 获取第一行的数据
		row = next(iter(reader))
		tr_id = row[0]
		lat = row[1]
		lon = row[2]
		# 写入cypher中
		cypher += ".addV('location')"\
			".property('lat','" + str(lat) + "')"\
			".property('lon','" + str(lon) + "')"
		# 存储经纬度信息
		tmp_lat = lat
		tmp_lon = lon
		local_client.submit(cypher)
		last_tr_id = tr_id
		for row in reader:
			if index == 1:
				index += 1
			# 获取属性
			tr_id = row[0]
			lat = row[1]
			lon = row[2]
			# 查询图中是否存在该节点
			query_cypher = "g.V()"\
				".has('lat','" + str(lat) + "')"\
				".has('lon','" + str(lon) + "')"
			re_v = local_client.submit(query_cypher)
			try:
				# 如果存在
				re_v = next(iter(re_v))
				# 记录这个节点
				adding_cypher = "g.V(" + str(re_v[0].id) + ")"\
					".as('" + str(index) + "')"
				# 判断它是否为首节点
				if tr_id != last_tr_id:
					tmp_lat = lat
					tmp_lon = lon
					last_tr_id = tr_id
				else:
					# 如果不是首节点，要查询上一个节点然后进行连接
					# 判断他和前一个节点之间有没有边
					adding_cypher += ".in()"\
						".has('lat','" + str(tmp_lat) + "')"\
						".has('lon','" + str(tmp_lon) + "')"
					print(adding_cypher)
					re_e = local_client.submit(adding_cypher)
					try:
						# 如果有这个边，那就把边的度加1
						re_e = next(iter(re_e))
						query_degree_cypher = "g.V(" + str(re_v[0].id) + ")"\
							".as('" + str(index) + "')"\
							".inE()"\
							".where(outV().has('lat','" + str(tmp_lat) + "')"\
							".has('lon','" + str(tmp_lon) + "'))"\
							".values('degree')"
						# 先获取其中的度
						re_d = local_client.submit(query_degree_cypher)
						re_d = next(iter(re_d))[0]
						print(type(re_d))
						# 度+1并更新节点属性
						re_d += 1
						adding_degree_cypher = "g.V(" + str(re_v[0].id) + ")"\
							".as('" + str(index) + "')"\
							".inE()"\
							".where(outV().has('lat','" + str(tmp_lat) + "')"\
							".has('lon','" + str(tmp_lon) + "'))"\
							".property('degree','" + str(re_d) + "')"
						local_client.submit(adding_degree_cypher)
					except:
						# 如果没有边，那就添加一个边
						cypher = "g.V(" + str(re_v[0].id) + ")"\
							".as('" + str(index) + "')"\
							".V()"\
							".has('lat','" + str(tmp_lat) + "')"\
							".has('lon','" + str(tmp_lon) + "')"\
							".addE('to').to('" + str(index) + "')"\
							".property('degree',1)"
					tmp_lon = lon
					tmp_lat = lat
			except:
				# 如果不存在已有的节点
				if last_tr_id == tr_id:
					# 如果不是首节点，那就添加节点和边
					cypher = "g.addV('location')"\
						".property('lat','" + str(lat) + "')"\
						".property('lon','" + str(lon) + "')"\
						".as('" + str(index) + "')"\
						".V()"\
						".has('lat','" + str(tmp_lat) + "')"\
						".has('lon','" + str(tmp_lon) + "')"\
						".addE('to').to('" + str(index) + "')"\
						".property('degree',1)"
					local_client.submit(cypher)
					tmp_lat = lat
					tmp_lon = lon
				else:
					# 如果是首节点，那就只添加节点
					cypher = "g.addV('location')"\
						".property('lat','" + str(lat) + "')"\
						".property('lon','" + str(lon) + "')"
					local_client.submit(cypher)
					last_tr_id = tr_id
					tmp_lon = lon
					tmp_lat = lat
			index += 1
	print(cypher)
	local_client.submit(cypher)





