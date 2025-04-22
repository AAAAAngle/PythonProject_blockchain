import pandas as pd
from datetime import datetime

def view_erc20():
    # 本地文件路径
    filename = "blockchair_erc-20_tokens_latest.tsv"
    print(f"正在读取本地文件: {filename}")
    
    try:
        # 直接读取本地文件
        with open(filename, 'r', encoding='latin1') as f:
            # 读取前50行
            df = pd.read_csv(f, sep='\t', nrows=50)
            
            # 显示列名
            print("\n数据列名：")
            for col in df.columns:
                print(f"- {col}")
            
            # 显示前50行数据
            print("\n前50行数据：")
            pd.set_option('display.max_columns', None)  # 显示所有列
            pd.set_option('display.width', None)  # 不限制显示宽度
            pd.set_option('display.max_colwidth', None)  # 不限制列宽
            print(df)
            
            # 保存预览到CSV文件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f'erc20_tokens_preview_{timestamp}.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            print(f"\n预览数据已保存到文件: {output_filename}")
            
    except FileNotFoundError:
        print(f"错误：找不到文件 {filename}")
    except Exception as e:
        print(f"处理数据时出错: {str(e)}")

if __name__ == "__main__":
    view_erc20() 
