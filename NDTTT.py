import time
import os
import io
from PIL import Image
import csv

# 采样的距离
step = 4
# 最小的阈值
min_pin = 2

pid = os.getpid()


"""
    对数据进行预处理：
    对坐标进行编号，扫描两次数据，以坐标为准则，如果数据相同之前遇到这个坐标了，说明此坐标已经写过，结束循环
    如果相同前没遇到坐标，其后的坐标写入，本身写入，最后计数加1。
"""


# 输出csv格式 人员序号 轨迹序号 时间戳 经度 纬度
if __name__ == '__main__':
    csvfile = "./data_part.csv"
    """
    第一次初始化，找出需要的列数据，对格式进行整理
    """
    f = open(csvfile, 'r')  # 打开这个csv 存在在变量f中  r read 读     w write 写   b 以二进制形式打开
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

    """
    这里存到内存中，memorylist可以方便后续的可视化过程
    """
    memory_list = []
    result_line_str = ""
    # 打开去重后的文件和编码的文件
    with open('./tem/reduce_data.csv') as f1:
        path = "./tem/init_data.csv"
        with open(path, 'w', newline="") as fd:
            reader1 = csv.reader(f1)
            writer1 = csv.writer(fd)
            # 遍历去重后的文件
            for row1 in reader1:
                # 读取三个参数
                truck_id = row1[0]
                x = row1[1]
                y = row1[2]
                # 构成坐标
                axis = (x, y)
                # 生成memory_list存储编码
                if memory_list:
                    count_memory_list = 0
                    flag = 0
                    for memory_list_axis in memory_list:
                        # 对memory_list进行遍历，判断坐标是否已经存在
                        count_memory_list = count_memory_list + 1
                        if memory_list_axis == axis:
                            # 如果存在，那就直接写入
                            writer1.writerow([row1[0], count_memory_list])
                            flag = 1
                            break
                    if flag == 0:
                        # 如果不存在，新加一个坐标
                        memory_list.append(axis)
                        writer1.writerow([row1[0], len(memory_list)])
                else:
                    # 若memory_list为空，那么生成memory_list
                    memory_list.append(axis)
                    writer1.writerow([row1[0], len(memory_list)])
        f1.close()
    fd.close()

    # 整理成：{车辆1：【路段1，路段2，路段3....】}
    # 读取并整合数据，将数据存储在alldatalist
    all_data_list = []
    # 打开编码好的文件
    fd = open("./tem/init_data.csv")
    for line in fd:
        flag = 0
        split_line = line.split(",")
        # 获取每一行的车辆id和位置id
        truck_id = split_line[0]
        space_id = split_line[1].split("\n")[0]
        # 如果alldatalist是空，那就加入数据
        if not all_data_list:
            all_data_list.append({truck_id: [space_id]})
            continue
        num = 0
        # 遍历alldatalist
        for truck_dist in all_data_list:
            # 如果存在车辆id
            if list(truck_dist.keys())[0] == truck_id:
                # 在这个车辆id中添加路段
                space_list = list(truck_dist.values())[0]
                space_list.append(space_id)
                truck_dist = {truck_id: space_list}
                all_data_list[num] = truck_dist
                flag = 1
                break
            num = num + 1
        # 如果没有这个车辆id，那就新建一个
        if flag == 0:
            all_data_list.append({truck_id: [space_id]})
    # print(all_data_list)

    # 将数据整理成：{轨迹段：车辆}
    trajectory_all_list = []
    # 遍历alldatalist
    for truck_dist in all_data_list:
        trajectory_list = []
        # 获取车辆id和轨迹id
        truck_id = list(truck_dist.keys())[0]
        space_list = list(truck_dist.values())[0]
        # 遍历这条轨迹
        for i in range(len(space_list) - 1):
            # 根据step进行取样
            trajectory_tuple = tuple(space_list[i:i + 2])
            trajectory_dist = {trajectory_tuple: [truck_id]}
            # 如果trajectoryalllist不为空，找到这个轨迹段的位置
            if trajectory_all_list:
                # 索引
                num = 0
                # 标志位判断trajectory_all_list中是否存在这个轨迹段
                flag = 0
                for a in trajectory_all_list:
                    # 判断轨迹段是否在其中存在
                    if tuple(list(a.keys())[0]) == trajectory_tuple:
                        # 如果存在那就更新
                        current_a = list(a.values())[0]
                        current_a.append(truck_id)
                        trajectory_dist = {trajectory_tuple: current_a}
                        trajectory_all_list[num] = trajectory_dist
                        flag = 1
                        break
                    num = num + 1
                # 如果不存在，那就新建一个轨迹段
                if flag == 0:
                    trajectory_all_list.append(trajectory_dist)
            # 如果trajectoryalllist为空，那就把这个轨迹段放进去
            else:
                trajectory_all_list.append(trajectory_dist)


    num = 0
    result = []
    # 遍历trajectory_all_list
    for i in trajectory_all_list:
        space_id = list(i.values())[0]
        # 根据阈值进行筛选
        if len(space_id) >= min_pin:
            result.append(i)
        num = num + 1
    #result是轨迹段编号+车辆编号（比如10000组数据，那每个车辆均有对应的组编号）
    # print(result)
# 保留二阶表
result_2 = result
# 生成后续的表
# 判断结果的长度是否到达距离阈值
while(len(list(result[0].keys())[0]) < step) and result != []:
    # 下一阶的结果
    new_result = []
    # 遍历上一阶结果
    for each_result in result:
        # 获取轨迹段
        trajectory_result = list(each_result.keys())[0]
        # 获取车辆集合
        vehicle_result = list(each_result.values())[0]
        # 遍历 车辆：轨迹段
        for data in all_data_list:
            # 获取轨迹段
            data_trajectory = list(data.values())[0]
            # 至少满足轨迹段大于某个人的轨迹
            if len(data_trajectory) >= len(trajectory_result):
                # 循环判断轨迹是否存在包含关系
                for i in range(len(data_trajectory) - len(trajectory_result) + 1):
                    # 如果存在包含关系,且不是最后以为
                    if(tuple(data_trajectory[i: i+len(trajectory_result)]) == trajectory_result) and ((i + len(trajectory_result)) < len(data_trajectory)):
                        # 新的轨迹
                        new_trajectory = data_trajectory[i: i + len(trajectory_result) + 1]
                        # print(new_trajectory)
                        # 根据原始数据判断
                        for other_result in result_2:
                            # 获取轨迹段
                            trajectory_other_result = list(other_result.keys())[0]
                            # 获取车辆集合
                            vehicle_other_result = list(other_result.values())[0]
                            # 是否存在相等关系
                            # print(data_trajectory[i+len(trajectory_result)-1 : i+len(trajectory_result)+1])
                            if trajectory_other_result == tuple(data_trajectory[i+len(trajectory_result)-1 : i+len(trajectory_result)+1]):
                                # 新的轨迹的车辆数
                                new_vehicle = [v for v in vehicle_result if v in vehicle_other_result]
                                # 判断是否满足频繁度阈值
                                if len(new_vehicle) >= min_pin:
                                    # 判断是否在最后列表中
                                    if {tuple(new_trajectory):new_vehicle} not in new_result:
                                        # 加入新列表
                                        new_result.append({tuple(new_trajectory):new_vehicle})

    result = new_result
    print(result)





