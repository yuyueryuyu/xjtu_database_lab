import pdfplumber
import pandas as pd
import os

def extract_tables_to_csv(pdf_path):
    """
    从PDF提取所有表格并保存为CSV文件
    """
    with pdfplumber.open(pdf_path) as pdf:
        first = True
        df = pd.DataFrame()
        for _, page in enumerate(pdf.pages, start=1):
            # 提取当前页的表格
            tables = page.extract_tables()
            
            # 跳过无表格的页
            if not tables:
                continue
            
            for _, table in enumerate(tables, start=1):
                if first:
                    df = pd.DataFrame(table)
                    first = False
                else:
                    # 将表格数据转换为DataFrame
                    df = pd.concat([df, pd.DataFrame(table)], ignore_index=True)
                
        # 保存为CSV
        df.to_csv("students_name.csv", index=False, encoding='utf-8')

if __name__ == "__main__":
    pdf_file = "2024国赛.pdf"
    extract_tables_to_csv(pdf_file)