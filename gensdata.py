import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict


def generate_student_id(used_ids):
    """
    生成不重复的8位学号
    格式0X0XXXXX
    """
    while True:
        # 第1位固定为0
        first = '0'
        # 第2位从1-9随机
        second = str(random.randint(1, 9))
        # 第3位固定为0
        third = '0'
        # 第4位从1-9随机
        fourth = str(random.randint(1, 9))
        # 后4位随机
        last_four = f"{random.randint(0, 9999):04d}"
        
        student_id = first + second + third + fourth + last_four
        
        if student_id not in used_ids:
            used_ids.add(student_id)
            return student_id


def generate_birth_date():
    """生成2001-01-01到2005-12-31之间的随机出生日期"""
    start_date = datetime(2001, 1, 1)
    end_date = datetime(2005, 12, 31)
    
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    
    birth_date = start_date + timedelta(days=random_days)
    return birth_date.strftime('%Y-%m-%d')

def generate_height(sex):
    """根据性别生成身高"""
    if sex == '男':
        # 男性：1.50-2.00，均值1.75
        height = np.random.normal(1.75, 0.08)
        height = max(1.50, min(2.00, height))
    else:
        # 女性：1.40-1.90，均值1.65
        height = np.random.normal(1.65, 0.08)
        height = max(1.40, min(1.90, height))
    
    return round(height, 2)

def generate_dorm(sex, dorm_counter, building_gender):
    """
    生成宿舍号，规则:
    1. 优先让宿舍住满
    2. 确保每个宿舍不超过4人
    3. 且男女不能住同一栋楼
    """
    
    # 首先尝试从已有的同性别宿舍中找未满的
    available_dorms = [dorm for dorm, count in dorm_counter.items() 
                      if count < 4 and get_building_from_dorm(dorm) in building_gender 
                      and building_gender[get_building_from_dorm(dorm)] == sex]
    
    # 按入住人数降序排序，优先选择人数较多的宿舍（让宿舍尽快住满）
    if available_dorms:
        available_dorms.sort(key=lambda x: dorm_counter[x], reverse=True)
        selected_dorm = available_dorms[0]
        dorm_counter[selected_dorm] += 1
        return selected_dorm
    
    # 如果没有同性别的未满宿舍，则创建新宿舍
    max_attempts = 1000
    attempts = 0
    
    while attempts < max_attempts:
        # 方位：东或西
        direction = random.choice(['东', '西'])
        # 舍号：1-30
        building = random.randint(1, 30)
        
        building_key = f"{direction}{building}舍"
        
        # 检查该楼是否已被另一性别占用
        if building_key in building_gender and building_gender[building_key] != sex:
            attempts += 1
            continue
        
        # 楼层：2-7
        floor = random.randint(2, 7)
        # 宿舍号：01-40
        room = random.randint(1, 40)
        
        dorm = f"{building_key}{floor}{room:02d}"
        
        # 确保这是一个新宿舍（未被使用过）
        if dorm not in dorm_counter:
            dorm_counter[dorm] = 1
            building_gender[building_key] = sex  # 记录该楼的性别
            return dorm
        
        attempts += 1

def get_building_from_dorm(dorm):
    """从宿舍号中提取楼栋信息"""
    import re
    match = re.match(r'([东西]\d+舍)', dorm)
    return match.group(1) if match else ""

def process_team_csv(input_file, output_file):
    """处理队员CSV文件并生成学生数据"""
    df = pd.read_csv(input_file)
    
    # 初始化已插入数据
    used_ids = {'01032010', '01032023', '01032001', '01032005', '01032112', 
                '03031011', '03031014', '03031051', '03031009', '03031033', '03031056'}
    dorm_counter = defaultdict(int)
    dorm_counter['东6舍221'] = 3
    dorm_counter['东1舍312'] = 2
    dorm_counter['东2舍104'] = 2
    dorm_counter['东18舍421'] = 1
    dorm_counter['东18舍422'] = 1
    dorm_counter['东18舍423'] = 1
    dorm_counter['东2舍305'] = 1
    building_gender = {}
    building_gender['东6舍'] = '男'
    building_gender['东18舍'] = '男'
    building_gender['东1舍'] = '女'
    building_gender['东2舍'] = '女'
    students_data = []
    
     # 为每个队员生成数据
    for _, row in df.iterrows():
        for _, col_name in enumerate(df.columns):
            member_name = str(row[col_name]).strip()
            if member_name and member_name != 'nan':
                # 生成学号
                student_id = generate_student_id(used_ids)
                
                # 生成性别
                sex = random.choice(['男', '女'])

                # 生成姓名
                sname = member_name
                
                # 生成出生日期
                bdate = generate_birth_date()
                
                # 生成身高
                height = generate_height(sex)
                
                # 生成宿舍
                dorm = generate_dorm(sex, dorm_counter, building_gender)
                
                students_data.append({
                    'S#': student_id,
                    'SNAME': sname,
                    'SEX': sex,
                    'BDATE': bdate,
                    'HEIGHT': height,
                    'DORM': dorm
                })
    
    # 创建DataFrame
    result_df = pd.DataFrame(students_data)
    
    # 保存到CSV文件
    result_df.to_csv(output_file, index=False, encoding='utf-8')

if __name__ == "__main__":

    input_filename = 'output_tables/page.csv'
    output_filename = 'students_data.csv'
    
    process_team_csv(input_filename, output_filename)