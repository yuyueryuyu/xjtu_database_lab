import pandas as pd
import numpy as np
import random

def generate_grade():
    """
    生成成绩，90%概率有成绩，10%概率为空
    服从正态分布，均值75，标准差10， 范围40-100
    """
    if random.random() < 0.9:
        grade = np.random.normal(75, 10)
        grade = max(40, min(100, grade))
        return round(grade, 1)
    else:
        return None

def select_courses_for_student(courses_df, target_credits):
    """为单个学生选择课程，使总学分接近目标学分"""
    selected_courses = []
    total_credits = 0
    available_courses = courses_df.copy()
    
    # 按学分排序，便于选择
    available_courses = available_courses.sort_values('CREDIT')
    
    attempts = 0
    max_attempts = 1000
    
    while total_credits < target_credits and len(available_courses) > 0 and attempts < max_attempts:
        attempts += 1
        
        # 计算剩余需要的学分
        remaining_credits = target_credits - total_credits
        
        # 筛选出不会超过剩余学分的课程
        suitable_courses = available_courses[available_courses['CREDIT'] <= remaining_credits]
        
        # 如果没有合适的课程了，随机选择一个最小学分的课程
        if len(suitable_courses) == 0:
            if len(available_courses) == 0:
                break
            course = available_courses.iloc[0]
            selected_courses.append(course['C#'])
            total_credits += course['CREDIT']
            break
        
        # 随机选择一门合适的课程
        course = suitable_courses.sample(n=1).iloc[0]
        selected_courses.append(course['C#'])
        total_credits += course['CREDIT']
        
        # 从可选课程中移除已选课程
        available_courses = available_courses[available_courses['C#'] != course['C#']]
    
    return selected_courses, total_credits

def generate_course_selection_table(students_file, courses_file, output_file, target_rows=200000):
    """生成选课表"""
    
    # 读取学生数据
    students_df = pd.read_csv(students_file, dtype=str)
    
    # 读取课程数据
    courses_df = pd.read_csv(courses_file)
    
    # 生成学分分布（正态分布，均值25，标准差8，范围0-60）
    num_students = len(students_df)
    credit_targets = np.random.normal(120, 30, num_students)
    credit_targets = np.clip(credit_targets, 0, 250)  # 限制在0-60范围内
    credit_targets = np.round(credit_targets, 1)
  
    # 生成选课记录
    selection_records = []
    student_credit_stats = []
    
    for idx, (_, student) in enumerate(students_df.iterrows()):
        student_id = student['S#']
        target_credits = credit_targets[idx]
        
        # 为该学生选择课程
        selected_courses, actual_credits = select_courses_for_student(courses_df, target_credits)
        
        # 记录该学生的选课统计
        student_credit_stats.append({
            'S#': student_id,
            'target_credits': target_credits,
            'actual_credits': actual_credits,
            'course_count': len(selected_courses)
        })
        
        # 为每门选中的课程生成选课记录
        for course_id in selected_courses:
            grade = generate_grade()
            selection_records.append({
                'S#': student_id,
                'C#': course_id,
                'GRADE': grade
            })
        
        if (idx + 1) % 100 == 0:
            print(f"已处理 {idx + 1}/{num_students} 名学生")
    
    # 创建选课表DataFrame
    selection_df = pd.DataFrame(selection_records)
    
    selection_df.to_csv(output_file, index=False)

# 示例用法
if __name__ == "__main__":

    # 生成选课表
    students_file = 'output_tables/students_5000.csv'
    courses_file = 'output_tables/courses_1000.csv'
    output_file = 'stu_course_200000.csv'
    
    generate_course_selection_table(
        students_file, 
        courses_file, 
        output_file, 
        target_rows=180500
    )