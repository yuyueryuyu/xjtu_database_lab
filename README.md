# XJTU Database Lab

西安交通大学数据库系统课内实验项目

## 项目结构

```
XJTU_DATABASE_LAB/
├── src/
│   ├── main/java/cn/edu/xjtu/dblab/
│   │   └── App.java                    # 主程序入口
│   └── test/java/cn/edu/xjtu/dblab/
│       └── AppTest.java                # 单元测试
├── pom.xml                             # Maven配置文件
│ 
│ # 数据处理脚本
├── parsepdf.py                     # PDF姓名提取脚本
├── gencdata.py                     # 数据格式处理脚本
├── genscdata.py                    # 选课关系生成脚本
├── request.py                      # 课程信息爬虫脚本
│ 
│ # 数据源文件
├── 2024国赛.pdf                    # 学生姓名数据源
│ 
│ # 生成数据
├── students_name.csv               # 提取的姓名表格
├── students_{1000,5000}.csv        # 学生数据（不同规模）
├── xjtu_courses.csv                # 爬取的课程信息
├── courses_{100,1000}.csv          # 课程数据（不同规模）
├── stu_course_{20000,200000}.csv   # 选课记录（不同规模）
│ 
│ # 数据库备份
├── 2223515348_backup.tar           # 基础实验备份
├── 2223515348_backup_trigger.tar   # 完整实验备份
│ 
└── 实验报告.pdf                     # 详细实验报告
```

## 数据处理流程

### 学生数据处理
1. `2024国赛.pdf` → `parsepdf.py` → `students_name.csv`
2. `students_name.csv` → `gencdata.py` → `students_{数字}.csv`

### 课程数据处理
1. 西安交通大学教务网站 → `request.py` → `xjtu_courses.csv`
2. `xjtu_courses.csv` → `gencdata.py` → `courses_{数字}.csv`

### 选课关系生成
1. `students_{数字}.csv` + `courses_{数字}.csv` → `genscdata.py` → `stu_course_{数字}.csv`

## 实验环境
- openEuler 20.03-LTS
- openjdk version 1.8.0_242
- Apache Maven 3.9.8
- Python 3.12.5
- openGauss 1.1.0

## 快速开始

### 1. 克隆项目
```bash
git clone https://github.com/yuyueryuyu/xjtu_database_lab.git
cd xjtu_database_lab
```

### 2. 构建Java项目
```bash
mvn clean package
```

### 3. 运行主程序
```bash
java -jar ./target/database-lab-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 数据集说明

包含多个规模的数据集：

| 数据类型 | 小规模 | 大规模 | 说明 |
|---------|--------|--------|------|
| 学生数据 | 1,000条 | 5,000条 | 包含姓名、学号等信息 |
| 课程数据 | 100门 | 1,000门 | 从教务网站爬取的真实数据 |
| 选课记录 | 20,000条 | 200,000条 | 学生选课关系 |

## 数据库备份

- `2223515348_backup.tar`: 基础实验完成后的数据库备份
- `2223515348_backup_trigger.tar`: 包含触发器等高级功能的完整备份