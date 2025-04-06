import pandas as pd
import numpy as np
from datetime import datetime
import os

def analyze_transactions():
    try:
        print("开始分析交易数据...")
        
        # 设置pandas选项以优化内存使用
        pd.options.mode.chained_assignment = None
        
        # 只读取需要的列，减少内存使用
        needed_columns = ['input_total_usd', 'is_coinbase', 'fee_usd']
        
        # 使用chunksize分块读取大文件
        chunk_size = 1000000  # 每次读取100万行
        input_zero_txs = []
        
        print("正在读取并处理数据...")
        for chunk in pd.read_csv('combined_transactions.csv', usecols=needed_columns, chunksize=chunk_size):
            # 处理input_total_usd为0的交易
            input_zero_mask = (chunk['input_total_usd'] == 0)
            input_zero_chunk = chunk[input_zero_mask].copy()
            if not input_zero_chunk.empty:
                input_zero_txs.append(input_zero_chunk)
            
            print(f"已处理 {len(input_zero_txs)} 个数据块...")
        
        # 合并结果
        if input_zero_txs:
            input_zero_df = pd.concat(input_zero_txs, ignore_index=True)
            input_zero_df = input_zero_df.sort_values('fee_usd')
            input_output_file = 'input_zero_transactions.csv'
            input_zero_df.to_csv(input_output_file, index=False, encoding='utf-8-sig')
            print(f"\n输入金额为0的交易已保存到: {input_output_file}")
            print(f"共找到 {len(input_zero_df)} 笔交易")
            
            # 显示统计信息
            print("\n输入金额为0的交易统计:")
            print(f"最小手续费: {input_zero_df['fee_usd'].min():.8f} USD")
            print(f"最大手续费: {input_zero_df['fee_usd'].max():.8f} USD")
            print(f"平均手续费: {input_zero_df['fee_usd'].mean():.8f} USD")
            
            # 显示is_coinbase的分布
            coinbase_stats = input_zero_df['is_coinbase'].value_counts()
            print("\nis_coinbase分布:")
            for value, count in coinbase_stats.items():
                print(f"is_coinbase={value}: {count}笔交易")
        else:
            print("未找到输入金额为0的交易")
            
    except FileNotFoundError:
        print("错误：找不到 combined_transactions.csv 文件")
    except Exception as e:
        print(f"处理过程中出错: {str(e)}")

if __name__ == "__main__":
    analyze_transactions()