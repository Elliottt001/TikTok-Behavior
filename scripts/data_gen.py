from faker import Faker
import csv
import json
import random
from datetime import datetime
import pathlib
import multiprocessing


class DataGenerator:

    def generate_chunk(self, chunk_id,
                       csv_record_count,
                       jsonl_record_count,
                       user_csv_headers,
                       video_csv_headers):

        raw_jsonl_file_path = pathlib.Path(f'../data/raw/user_behavior_logs'
                                           f'_chunk_{chunk_id}.jsonl')
        raw_user_csv_file_path = f'../data/raw/user_chunk_{chunk_id}.csv'
        raw_video_csv_file_path = f'../data/raw/video_chunk_{chunk_id}.csv'

        self.csv_gen(raw_user_csv_file_path,
                     csv_record_count,
                     self.get_user_record,
                     user_csv_headers)
        self.csv_gen(raw_video_csv_file_path,
                     csv_record_count,
                     self.get_video_record,
                     video_csv_headers)
        self.jsonl_gen(raw_jsonl_file_path, jsonl_record_count)

    def parallel_generate(self,
                          total_csv_record_count,
                          total_jsonl_record_count,
                          user_csv_headers,
                          video_csv_headers
                          ):

        num_cores = multiprocessing.cpu_count()
        csv_record_per_core = total_csv_record_count // num_cores
        jsonl_record_per_core = total_jsonl_record_count // num_cores

        processes = []

        for i in range(num_cores):
            p = multiprocessing.Process(target=self.generate_chunk,
                                        args=(i,
                                              csv_record_per_core,
                                              jsonl_record_per_core,
                                              user_csv_headers,
                                              video_csv_headers
                                              ))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

    fake = Faker(['zh_CN'])

    # 预定义数据列表，模拟真实的文本
    VIDEO_TAGS = ["搞笑", "生活", "美食", "旅行", "科技", "游戏", "美妆", "运动", "萌宠",
                  "音乐", "舞蹈", "影视", "知识", "二次元", "职场", "情感", "汽车", "房产",
                  "育儿", "健康", "科普", "历史", "文化", "手工", "绘画", "摄影", "编程",
                  "AI", "创业", "理财", "股市", "基金", "保险", "法律", "心理", "星座",
                  "八卦", "娱乐", "明星", "综艺", "电影", "电视剧", "动漫", "小说", "文学",
                  "艺术", "设计", "装修", "家居", "园艺", "钓鱼", "户外", "露营", "滑雪",
                  "潜水", "冲浪", "攀岩", "跑步", "瑜伽", "健身", "减肥", "养生", "中医",
                  "西医", "牙科", "眼科", "整容", "护肤", "穿搭", "发型", "美甲", "婚礼",
                  "恋爱", "相亲", "结婚", "离婚", "家庭", "婆媳", "亲子", "教育", "留学",
                  "考研", "考公", "求职", "面试", "办公", "Excel", "PPT", "Python",
                  "Java", "C++", "Rust", "Linux", "Windows", "Mac", "iOS",
                  "Android", "华为", "小米", "苹果", "三星", "索尼", "任天堂", "微软",
                  "谷歌", "亚马逊", "特斯拉", "SpaceX", "NASA", "宇宙", "天文", "地理",
                  "物理", "化学", "生物", "数学", "英语", "日语", "韩语", "法语",
                  "德语", "西班牙语", "俄语", "意大利语"]
    TITLE_TEMPLATES = [
        "{adj}的{noun}",
        "挑战{verb}{noun}",
        "今天去{place}{verb}",
        "关于{noun}的{number}个真相",
        "沉浸式{verb}",
        "{adj}！{noun}大测评",
        "我的{time}日常",
        "学会这个{noun}，{result}",
        "没想到{place}竟然有{noun}",
        "{noun}初体验，{result}",
        "在{place}{verb}{noun}是种什么体验？",
        "{number}天{verb}{noun}挑战",
        "揭秘{noun}背后的{adj}故事",
        "{place}必吃！{adj}的{noun}",
        "如何用{noun}{verb}？",
        "{adj}！{place}的{noun}太{adj}了",
        "带你{verb}{place}的{noun}",
        "{time}，我{verb}了{noun}",
        "{noun}VS{noun}，谁更{adj}？",
        "千万不要在{place}{verb}{noun}",
        "{noun}的{number}种{adj}用法",
        "我是如何{verb}{noun}的",
        "{place}流浪记：{verb}{noun}",
        "{adj}警告！{noun}千万别{verb}",
        "为了{noun}，我去了{place}",
        "{noun}大赏：{number}款{adj}好物",
        "跟{noun}说拜拜，{result}",
        "{place}探店：{adj}的{noun}",
        "{noun}避坑指南",
        "{noun}保姆级教程",
        "{noun}天花板！",
        "这才是{adj}的{noun}",
        "被{noun}治愈的一天",
        "{noun}界的新物种",
        "{noun}还能这样{verb}？",
        "{place}偶遇{noun}",
        "{noun}让我{result}",
        "{noun}救星来了",
        "{noun}正确打开方式",
        "{noun}深度解析"
    ]
    ADJECTIVES = ["惊人", "好玩", "美味", "神秘", "超强", "感动", "尴尬", "真实", "硬核",
                  "绝美", "奇葩", "恐怖", "搞笑", "治愈", "解压", "烧脑", "实用", "昂贵",
                  "便宜", "免费", "限量", "独家", "首发", "最新", "复古", "怀旧", "经典",
                  "流行", "网红", "小众", "冷门", "热门", "必看", "必吃", "必玩", "必买",
                  "后悔", "值得", "坑爹", "良心", "黑心", "暴力", "温柔", "可爱", "帅气",
                  "漂亮", "丑陋", "奇怪", "正常", "疯狂", "理智", "聪明", "愚蠢", "简单",
                  "困难", "复杂", "容易", "快速", "慢速", "高效", "低效", "安全", "危险",
                  "健康", "有害", "环保", "浪费", "奢侈", "节约", "富有", "贫穷", "开心",
                  "难过", "愤怒", "平静", "焦虑", "放松", "紧张", "刺激", "无聊", "有趣",
                  "精彩", "糟糕", "完美", "残缺", "巨大", "微小", "长", "短", "高", "矮",
                  "胖", "瘦", "美", "丑", "香", "臭", "甜", "苦", "辣", "咸", "酸"]
    NOUNS = ["挑战", "美食", "风景", "秘密", "技巧", "猫咪", "手机", "游戏", "故事", "经历",
             "穿搭", "黑科技", "狗狗", "电脑", "键盘", "鼠标", "耳机", "音响", "相机", "镜头",
             "无人机", "手表", "手环", "眼镜", "衣服", "裤子", "鞋子", "帽子", "包包", "化妆品",
             "护肤品", "香水", "口红", "面膜", "洗发水", "沐浴露", "牙膏", "牙刷", "毛巾", "床单",
             "被子", "枕头", "沙发", "桌子", "椅子", "柜子", "冰箱", "洗衣机", "空调", "电视",
             "投影仪", "扫地机", "吸尘器", "吹风机", "卷发棒", "剃须刀", "指甲刀", "剪刀", "胶水",
             "胶带", "纸巾", "湿巾", "垃圾袋", "垃圾桶", "拖把", "扫把", "抹布", "洗洁精", "洗衣液",
             "柔顺剂", "消毒液", "洗手液", "沐浴球", "搓澡巾", "浴巾", "浴袍", "睡衣", "内衣", "内裤",
             "袜子", "丝袜", "领带", "皮带", "围巾", "手套", "口罩", "墨镜", "雨伞", "雨衣", "雨鞋",
             "帐篷", "睡袋", "背包", "行李箱", "钱包", "卡包", "钥匙扣", "手机壳", "充电宝", "数据线",
             "充电器", "插座", "电池", "灯泡", "台灯", "吊灯", "壁灯", "落地灯", "手电筒", "蜡烛",
             "打火机", "火柴", "烟灰缸", "花瓶", "花盆", "鲜花", "干花", "绿植", "多肉", "仙人掌",
             "盆景", "鱼缸", "金鱼", "乌龟", "仓鼠", "兔子", "鹦鹉", "鸽子"]
    VERBS = ["吃", "看", "玩", "做", "去", "体验", "尝试", "学习", "打卡", "测评", "购买",
             "使用", "拆箱", "改造", "修理", "清洁", "整理", "收纳", "烹饪", "烘焙", "烧烤",
             "火锅", "野餐", "露营", "旅行", "出差", "上班", "下班", "加班", "开会", "面试",
             "辞职", "求职", "创业", "投资", "理财", "存钱", "花钱", "借钱", "还钱", "捐款",
             "众筹", "网购", "逛街", "看电影", "看书", "听音乐", "唱歌", "跳舞", "画画", "写字",
             "拍照", "录像", "剪辑", "直播", "连麦", "PK", "抽奖", "送礼", "打赏", "点赞", "评论",
             "转发", "收藏", "关注", "取关", "拉黑", "举报", "投诉", "反馈", "建议", "咨询", "预约",
             "挂号", "排队", "等位", "点餐", "买单", "结账", "退款", "退货", "换货", "维修", "保养",
             "洗车", "加油", "充电", "停车", "违章", "罚款", "扣分", "考驾照", "买车", "卖车", "租车",
             "打车", "坐公交", "坐地铁", "坐火车", "坐飞机", "坐船", "骑车", "走路", "跑步", "游泳",
             "爬山", "滑雪", "溜冰"]
    PLACES = ["北京", "上海", "家里", "学校", "公司", "公园", "海边", "超市", "健身房", "广州",
              "深圳", "杭州", "成都", "重庆", "武汉", "西安", "南京", "苏州", "天津", "长沙",
              "郑州", "东莞", "青岛", "沈阳", "宁波", "昆明", "无锡", "佛山", "合肥", "大连",
              "福州", "厦门", "哈尔滨", "济南", "温州", "南宁", "长春", "泉州", "石家庄", "贵阳",
              "南昌", "金华", "常州", "南通", "嘉兴", "太原", "徐州", "惠州", "珠海", "中山",
              "台州", "烟台", "兰州", "绍兴", "海口", "扬州", "汕头", "湖州", "盐城", "潍坊",
              "保定", "镇江", "洛阳", "泰州", "乌鲁木齐", "临沂", "唐山", "漳州", "赣州", "廊坊",
              "呼和浩特", "芜湖", "桂林", "银川", "揭阳", "三亚", "遵义", "江门", "济宁", "莆田",
              "湛江", "绵阳", "淮安", "连云港", "淄博", "宜昌", "邯郸", "上饶", "柳州", "九江",
              "襄阳", "宁德", "阜阳", "衡阳", "岳阳", "新乡", "邢台", "南阳", "滁州", "株洲",
              "商丘", "信阳", "宿迁", "肇庆", "德州", "威海", "安庆", "清远", "荆州", "周口",
              "马鞍山", "沧州", "信阳", "衡水", "大庆", "长治", "衡阳", "秦皇岛", "吉林", "常德",
              "益阳", "娄底", "郴州", "永州", "怀化", "湘西", "张家界", "湘潭", "邵阳"]
    RESULTS = ["绝了", "后悔没早知道", "太好用了", "笑死我了", "泪目", "真香", "翻车了", "惊呆了",
               "吓死宝宝了", "美哭了", "好吃到哭", "难吃到吐", "太坑了", "智商税", "yyds", "绝绝子",
               "无语子", "也是醉了", "心态崩了", "破防了", "上头了", "下头了", "社死了", "emo了",
               "芭比Q了", "这就尴尬了", "也是没谁了", "太难了", "我太难了", "这就离谱", "离大谱",
               "也是服了", "这就很棒", "这就很nice", "这就很舒服", "这就很灵性", "这就很关键",
               "这就很细节", "这就很真实", "这就很尴尬", "这就很无语", "这就很离谱", "这就很神奇",
               "这就很意外", "这就很惊喜", "这就很感动", "这就很扎心", "这就很暖心", "这就很治愈",
               "这就很解压", "这就很烧脑", "这就很恐怖", "这就很刺激", "这就很无聊", "这就很有趣",
               "这就很精彩", "这就很糟糕", "这就很完美", "这就很残缺", "这就很巨大", "这就很微小"]

    user_pool = []
    video_pool = []

    def csv_gen(self, file_name, record_count, record_func, headers):
        """
        file_name: 文件名
        record_count: 行数
        record_func:  生成单行数据的函数对象
        headers:   表头列表
        """
        with open(file_name, 'w', encoding='UTF-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            for i in range(record_count):
                writer.writerow(record_func())
                if (record_count + 1) % 100000 == 0:
                    print(f"已往 {file_name} 写入 {i + 1} 条数据")

    def get_random_title(self):
        template = random.choice(self.TITLE_TEMPLATES)
        return template.format(
            adj=random.choice(self.ADJECTIVES),
            noun=random.choice(self.NOUNS),
            verb=random.choice(self.VERBS),
            place=random.choice(self.PLACES),
            number=random.randint(3, 10),
            time=self.fake.day_of_week(),
            result=random.choice(self.RESULTS)
        )

    def get_user_record(self):
        phone_type_tuple = ("iPhone 14", "iPhone 13", "HUAWEI Mate 60", "HUAWEI P60", "OPPO Find X", 'VIVO X90', "Xiaomi 13", "Honor 90")
        
        # 更像真实用户的昵称
        if random.random() < 0.4:
            nickname = fake.name()  # 真实姓名风格
        elif random.random() < 0.7:
            nickname = fake.word() + str(random.randint(100, 9999)) # 词语+数字
        else:
            nickname = random.choice(ADJECTIVES) + random.choice(NOUNS) # 形容词+名词

        # 大部分人粉丝少，少数人粉丝多
        if random.random() < 0.9:
            fans = random.randint(0, 5000)
        else:
            fans = random.randint(5000, 1000000)

        user_dict = {
            "user_id": None if random.random() < 0.02 else fake.random_number(digits=11), # 加入脏数据：None 的USER ID
            "nickname": None if random.random() < 0.02 else nickname, # 加入脏数据
            "age": fake.random_int(min=12, max=60),
            "ip": fake.city_name(),
            "fans_num": fans,
            "likes_num": fake.random_int(min=0, max=2000),
            "phone_type": fake.random_element(elements=phone_type_tuple),
            "register_time": fake.date_between(start_date='-5y', end_date='today').strftime("%Y/%m/%d")
            }
        
        # 生成脏数据：重复
        if random.random() < 0.01:
            user_pool.append(user_dict)
            user_pool.append(user_dict)

        user_pool.append(user_dict) 

        values = list(user_dict.values())
        return values

    def get_video_record(self):
        # 随机选择1-3个标签
        num_tags = random.randint(1, 3)
        tag_tuple = tuple(random.sample(VIDEO_TAGS, num_tags))

        # 视频时长分布 (短视频为主)
        r = random.random()
        if r < 0.7:
            duration = random.randint(10, 60)  # 10-60秒
        elif r < 0.95:
            duration = random.randint(60, 300)  # 1-5分钟
        else:
            duration = random.randint(300, 1800)  # 长视频

        video_dict = {
            "user_id": fake.random_number(digits=11),
            "video_id": fake.random_number(digits=13),
            "title": get_random_title(),
            "tags": tag_tuple,
            "duration": duration,
            "update_time": fake.date_between(start_date='-2y', end_date='today').strftime("%Y/%m/%d") 
        }

        video_pool.append(video_dict)

        values = list(video_dict.values())
        return values

    def jsonl_gen(self, file_name, logs_count):
        
        # 用户 / 视频分桶：模拟热门视频 / 活跃用户
        hot_video_rate = 0.1
        hot_user_rate = 0.05

        hot_video_count = int(len(self.video_pool) * hot_video_rate)
        hot_user_count = int(len(self.user_pool) * hot_user_rate)

        hot_videos = self.video_pool[:hot_video_count]
        cold_videos = self.video_pool[hot_video_count:]

        hot_users = self.user_pool[:hot_user_count]
        cold_users = self.user_pool[hot_user_count:]

        with open(file_name, 'w', encoding="UTF-8") as file:
            for i in range(0, logs_count):
                if random.random() < 0.5:
                    action_type = "view"
                elif random.random() >= 0.5 and random.random() < 0.7:
                    action_type = "like"
                elif random.random() >= 0.7 and random.random() < 0.9:
                    action_type = "comment"
                else:
                    action_type = "follow"

                if random.random() < 0.8:
                    v = random.choice(hot_videos)
                else:
                    v = random.choice(cold_videos)

                if random.random() < 0.7:
                    u = random.choice(hot_users)
                else:
                    u = random.choice(cold_users)

                if random.random() < 0.02:
                    duration = random.randint(-1000, -1)  # 脏数据：负的观看时长
                elif random.random() >= 0.02 and random.random() < 0.6:
                    duration = v["duration"] * 0.3
                elif random.random() >= 0.6 and random.random() < 0.9:
                    duration = v["duration"] * 0.7
                else:
                    duration = v["duration"]

                data = {
                    "user_id": u["user_id"],
                    "video_id": v["video_id"],
                    "action_type": action_type,
                    "duration": duration,
                    "time_stamp": self.fake.date_time_between(
                        start_date=datetime.strptime(v["update_time"],
                                                     "%Y/%m/%d"),
                        end_date='now').strftime("%Y/%m/%d %H:%M:%S"),
                    "ip": self.fake.city_name()
                }

                file.write(json.dumps(data, ensure_ascii=False) + '\n')

                if (i + 1) % 1000000 == 0:
                    print(f"已往 {file_name} 写入 {i + 1} 条数据")
