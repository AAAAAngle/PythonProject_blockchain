from datetime import datetime, timedelta
import gzip
import io
import re
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup


def parse_file_info(html_content):
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # 查找所有链接
    file_info_list = []

    # 将HTML内容按行分割
    lines = html_content.split('\n')
    for line in lines:
        if 'blockchair_bitcoin_transactions_' in line and '.tsv.gz' in line:
            # 使用更精确的正则表达式匹配整行内容
            pattern = r'<a href="(blockchair_bitcoin_transactions_\d{8}\.tsv\.gz)">.*?</a>\s+(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})\s+(\d+[KM]?)'
            match = re.search(pattern, line)
            if match:
                filename = match.group(1)
                date_str = match.group(2)
                size = match.group(3)
                file_info_list.append({
                    'filename': filename,
                    'date': date_str,
                    'size': size
                })

    print("找到的文件信息：")
    for info in file_info_list:
        print(f"文件名: {info['filename']}, 日期: {info['date']}, 大小: {info['size']}")

    return file_info_list


def download_and_process_data(start_date, end_date):
    # 创建保存数据的目录
    if not os.path.exists('blockchair_data'):
        os.makedirs('blockchair_data')

    # 首先获取网页内容
    url = "https://gz.blockchair.com/bitcoin/transactions/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    print("111111")
    try:
        print("正在尝试访问网页...")
        response = requests.get(url, headers=headers, timeout=10)
        print(f"HTTP状态码: {response.status_code}")

        if response.status_code == 200:
            print("成功获取网页内容")
            print("响应头信息：")
            print(response.headers)
            print("响应内容预览：")
            print(response.text[:500])  # 打印前500个字符
            file_info_list = parse_file_info(response.text)
            print(f"找到 {len(file_info_list)} 个文件信息")

            # 过滤日期范围内的文件
            filtered_files = []
            for file_info in file_info_list:
                file_date = datetime.strptime(file_info['date'], '%d-%b-%Y %H:%M')
                if start_date <= file_date <= end_date:
                    filtered_files.append(file_info)

            print(f"在指定日期范围内找到 {len(filtered_files)} 个文件")

            # 下载并处理文件
            all_data = []
            for file_info in filtered_files:
                filename = file_info['filename']
                download_url = f"https://gz.blockchair.com/bitcoin/transactions/{filename}"

                try:
                    print(f"正在下载 {filename}...")
                    response = requests.get(download_url)

                    if response.status_code == 200:
                        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                            df = pd.read_csv(gz, sep='\t')
                            all_data.append(df)
                            print(f"成功处理 {filename}")
                    else:
                        print(f"下载失败 {filename}: HTTP {response.status_code}")

                except Exception as e:
                    print(f"处理 {filename} 时出错: {str(e)}")

            # 合并所有数据
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                output_file = 'blockchair_data/combined_transactions.csv'
                combined_df.to_csv(output_file, index=False)
                print(f"所有数据已保存到 {output_file}")
            else:
                print("没有找到任何数据")
        else:
            print(f"获取文件列表失败: HTTP {response.status_code}")
    except Exception as e:
        print(f"获取文件列表时出错: {str(e)}")


if __name__ == "__main__":
    # 设置日期范围
    start_date = datetime(2022, 7, 1)
    end_date = datetime(2022, 10, 1)

    download_and_process_data(start_date, end_date)