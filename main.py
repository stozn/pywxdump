import csv
import json
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pywxdump import *
from pywxdump.db import DBHandler

# 修改群聊名称
GROUP_NAME = 'Drrr桌游分队'

# 读取微信信息
info = get_wx_info(WX_OFFS)[0]

wx_dir = info['wx_dir']
key = info['key']
db_path = os.path.join(os.path.abspath(os.getcwd()), "data.db")
out_path = "./"  # 输出路径（目录）
flag = True

if os.path.exists(db_path):
    if input("数据库解密文件已存在，是否重新生成？(Y/N): ").lower() == 'n':
        flag = False

# 解密并合并微信数据库
if flag:
    decrypt_merge(wx_dir, key, out_path, db_path)

my_wxid = info['wxid']
db_config = {
    "key": key,
    "type": "sqlite",
    "path": db_path
}

outpath = "./"
page_size = 1000000

# 初始化数据库处理器
db = DBHandler(db_config, my_wxid)
roomId = None

# 获取房间列表和会话列表
room_list = db.get_room_list()
session_list = db.get_session_list()

# 查找目标群聊的ID
for id, data in session_list.items():
    if data['strNickName'] == GROUP_NAME:
        roomId = id
        break

# 获取房间内的用户信息
userInfo = room_list[roomId]['wxid2userinfo']

with open('userInfo.json', 'w', encoding='utf-8') as f:
    json.dump(userInfo, f, ensure_ascii=False, indent=4)

# 重新初始化数据库处理器
db = DBHandler(db_config, my_wxid)

# 获取聊天记录总数
count = db.get_msgs_count(roomId)
chatCount = count.get(roomId, 0)

# 调整页面大小以适应聊天记录总数
if page_size > chatCount:
    page_size = chatCount + 1

# 用于存储每个用户的发言次数和最后发言时间
user_stats = {}

# 构建用户信息字典
users = {user['wxid']: {'wxid': user['wxid'], 'nickname': user['nickname'], 'remark': user['remark'], 'headImgUrl': user['headImgUrl']} for user in userInfo.values()}

# 遍历聊天记录以更新用户统计数据
for i in range(0, chatCount, page_size):
    start_index = i
    data, _ = db.get_msgs(roomId, start_index, page_size)
    
    save_path = os.path.join(outpath, f"聊天记录.csv")

    with open(save_path, "w", encoding="utf-8-sig", newline='') as f:  # 使用追加模式写入CSV文件，并添加UTF-8 BOM
        csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

        # 写入表头仅在第一次写入时执行
        if start_index == 0:
            csv_writer.writerow(["微信ID", "昵称", "备注", "类型", "消息", "时间", "头像"])

        for row in data:
            wxid = row.get("talker", "")
            if wxid in users.keys():
                nickname = users[wxid]['nickname']
                remark = users[wxid]['remark']
                imageUrl = users[wxid]['headImgUrl']
                type_name = row.get("type_name", "")
                msg = row.get("msg", "")
                CreateTime = row.get("CreateTime", "")
                csv_writer.writerow([wxid, nickname, remark, type_name, msg, CreateTime, imageUrl])

                if wxid not in user_stats:
                    user_stats[wxid] = {"count": 0, "last_message_time": None}

                user_stats[wxid]["count"] += 1

                if user_stats[wxid]["last_message_time"] is None or CreateTime > user_stats[wxid]["last_message_time"]:
                    user_stats[wxid]["last_message_time"] = CreateTime

# 对于没有发言记录的用户，设置默认值
for wxid, user_info in users.items():
    if wxid not in user_stats:
        user_stats[wxid] = {"count": 0, "last_message_time": '1111-11-11 11:11:11'}

# 计算不活跃天数
current_date = datetime.now()
for wxid, stats in user_stats.items():
    last_message_time = stats["last_message_time"]
    if last_message_time != '1111-11-11 11:11:11':
        last_message_datetime = datetime.strptime(last_message_time, "%Y-%m-%d %H:%M:%S")
        inactive_days = (current_date - last_message_datetime).days
    else:
        inactive_days = None
    user_stats[wxid]["inactive_days"] = inactive_days

save_path = os.path.join(outpath, f"数据统计.csv")
with open(save_path, "w", encoding="utf-8-sig", newline='') as f:  # 添加UTF-8 BOM
    csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

    csv_writer.writerow(["微信ID", "昵称", "备注", "发言次数", "最后发言时间", "不活跃天数", "头像"])

    for wxid, stats in sorted(user_stats.items(), key=lambda x: x[1]["last_message_time"], reverse=True):
        nickname = users[wxid]['nickname']
        remark = users[wxid]['remark']
        imageUrl = users[wxid]['headImgUrl']
        counts = stats["count"]
        last_message_time = stats["last_message_time"]
        inactive_days = stats["inactive_days"]
        csv_writer.writerow([wxid, nickname, remark, counts, last_message_time, inactive_days, imageUrl])
