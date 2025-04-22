import pandas as pd
import os
import glob
from datetime import datetime

def merge_excel_files():
    try:
        # 获取data目录下所有Excel文件
        excel_files = glob.glob('data/*.xlsx') + glob.glob('data/*.xls')
        
        if not excel_files:
            print("data目录下没有找到Excel文件")
            return
        
        print(f"找到 {len(excel_files)} 个Excel文件")
        
        # 读取所有Excel文件
        all_data = []
        for file in excel_files:
            try:
                print(f"正在读取文件: {file}")
                df = pd.read_excel(file)
                all_data.append(df)
                print(f"成功读取文件: {file}")
            except Exception as e:
                print(f"读取文件 {file} 时出错: {str(e)}")
        
        if not all_data:
            print("没有成功读取任何文件")
            return
        
        # 合并所有数据
        print("开始合并数据...")
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'merged_data_{timestamp}.xlsx'
        
        # 保存合并后的数据
        print(f"正在保存合并后的数据到文件: {output_file}")
        merged_df.to_excel(output_file, index=False)
        
        print(f"\n合并完成！")
        print(f"共合并 {len(excel_files)} 个文件")
        print(f"总记录数: {len(merged_df)}")
        print(f"数据已保存到文件: {output_file}")
        
    except Exception as e:
        print(f"程序运行出错: {str(e)}")

if __name__ == "__main__":
    merge_excel_files() 
