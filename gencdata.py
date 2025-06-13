import pandas as pd
import random

def convert_course_code(kch):
    """
    将西交课程代码转换为要求格式
    [2位专业]-[2位序号]
    """
    # 提取第一个字母、第三个字母和中间数字
    first_letter = kch[0]
    third_letter = kch[2]
    major = first_letter + third_letter
    # CS 专业按照这个规则会变成CM，进行特殊处理
    if kch[:4] == 'COMP':
        major = 'CS'
    two_digits = kch[-4:-2]
    
    return f"{major}-{two_digits}"

def extract_first_teacher(teachers_str):
    """
    提取第一个教师姓名
    """
    if pd.isna(teachers_str) or teachers_str == '':
        return ''
    
    # 按逗号分割并取第一个
    teachers = teachers_str.split(',')
    return teachers[0].strip()

def process_csv(input_file, output_file):
    """
    处理CSV文件转换
    """
    df = pd.read_csv(input_file)
    
    # 初始化已插入数据
    new_data = []
    # 用于检查重复的课程代码
    used_codes = {'CS-01', 'CS-02', 'CS-03', 'CS-04', 'CS-05', 'EE-01', 'EE-02', 'EE-03'}
    
    for _, row in df.iterrows():
        kch = str(row['KCH']).strip()
        kcm = str(row['KCM']).strip()
        skjs = str(row['SKJS']).strip()
        xf = str(row['XF']).strip()
        xs = str(row['XS']).strip()
        
        # 生成新的课程代码
        course_code = convert_course_code(kch)
        
        # 确保课程代码不重复
        if course_code in used_codes:
            continue
        used_codes.add(course_code)
        
        # 生成其他字段
        period = xs
        credit = xf
        teacher = extract_first_teacher(skjs)
        
        new_data.append({
            'C#': course_code,
            'CNAME': kcm,
            'PERIOD': period,
            'CREDIT': credit,
            'TEACHER': teacher
        })
    
    # 创建新的DataFrame
    new_df = pd.DataFrame(new_data)
    
    # 保存到新的CSV文件
    new_df.to_csv(output_file, index=False, encoding='utf-8')

if __name__ == "__main__":
    
    # 处理文件
    input_filename = 'output_tables/111.csv'
    output_filename = 'output_courses.csv'
    
    process_csv(input_filename, output_filename)

