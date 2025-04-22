from datetime import datetime, timedelta
import gzip
import io
import re
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time


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


def open_file(file_path):
    """打开文件供查看"""
    try:
        # 获取绝对路径
        abs_path = os.path.abspath(file_path)
        print(f"\n文件已保存在: {abs_path}")

        if os.path.exists(abs_path):
            # Windows系统下使用默认程序打开文件
            os.startfile(abs_path)
            print("已为您打开文件，请在默认程序中查看")
        else:
            print(f"错误：文件 {abs_path} 不存在")
    except Exception as e:
        print(f"打开文件时出错: {str(e)}")


def download_and_process_data(start_date, end_date):
    # 创建保存数据的目录
    output_dir = 'blockchair_data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建数据目录: {os.path.abspath(output_dir)}")

    # 设置输出文件和记录文件的路径
    output_file = os.path.join(output_dir, 'combined_transactions.csv')
    download_record_file = os.path.join(output_dir, 'downloaded_files.txt')

    # 读取已下载文件记录
    downloaded_files = set()
    if os.path.exists(download_record_file):
        with open(download_record_file, 'r') as f:
            downloaded_files = set(line.strip() for line in f)
        print(f"发现已下载文件记录，已处理: {len(downloaded_files)} 个文件")

    # 获取文件列表
    url = "https://gz.blockchair.com/bitcoin/transactions/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        print("正在获取文件列表...")
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            file_info_list = parse_file_info(response.text)
            print(f"找到 {len(file_info_list)} 个文件信息")

            # 过滤日期范围内的文件
            filtered_files = []
            for file_info in file_info_list:
                file_date = datetime.strptime(file_info['date'], '%d-%b-%Y %H:%M')
                if start_date <= file_date <= end_date:
                    filtered_files.append(file_info)

            total_files = len(filtered_files)
            remaining_files = [f for f in filtered_files if f['filename'] not in downloaded_files]
            print(f"在指定日期范围内找到 {total_files} 个文件，其中 {len(remaining_files)} 个需要下载")

            if not remaining_files:
                print("所有文件已下载完成！")
                open_file(output_file)
                return

            # 下载并处理文件
            failed_files = []  # 记录失败的文件
            for file_info in remaining_files:
                filename = file_info['filename']
                download_url = f"https://gz.blockchair.com/bitcoin/transactions/{filename}"

                try:
                    print(f"\n正在下载 {filename}...")
                    response = requests.get(download_url, stream=True, timeout=30)

                    if response.status_code == 200:
                        try:
                            # 解压并处理文件
                            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                                df = pd.read_csv(gz, sep='\t')

                                # 如果是第一个文件且文件不存在，写入表头
                                if not os.path.exists(output_file):
                                    df.to_csv(output_file, index=False)
                                    print(f"创建新文件: {os.path.abspath(output_file)}")
                                else:
                                    # 追加数据，不写入表头
                                    df.to_csv(output_file, mode='a', header=False, index=False)
                                    print(f"追加到文件: {os.path.abspath(output_file)}")

                                # 显示当前总文件大小
                                file_size = os.path.getsize(output_file) / (1024 * 1024)  # 转换为MB
                                print(f"当前文件大小: {file_size:.2f} MB")

                                # 记录已下载的文件
                                with open(download_record_file, 'a') as f:
                                    f.write(f"{filename}\n")
                                downloaded_files.add(filename)

                                print(f"成功处理并保存 {filename}")
                        except Exception as e:
                            error_msg = f"处理文件 {filename} 时出错: {str(e)}"
                            print(error_msg)
                            failed_files.append((filename, error_msg))
                    elif response.status_code == 402:
                        error_msg = f"下载 {filename} 失败: 需要付费访问 (HTTP 402)"
                        print(error_msg)
                        failed_files.append((filename, error_msg))
                    else:
                        error_msg = f"下载 {filename} 失败: HTTP {response.status_code}"
                        print(error_msg)
                        failed_files.append((filename, error_msg))

                except requests.Timeout:
                    error_msg = f"下载 {filename} 超时"
                    print(error_msg)
                    failed_files.append((filename, error_msg))
                except requests.RequestException as e:
                    error_msg = f"下载 {filename} 请求错误: {str(e)}"
                    print(error_msg)
                    failed_files.append((filename, error_msg))
                except Exception as e:
                    error_msg = f"处理 {filename} 时发生未知错误: {str(e)}"
                    print(error_msg)
                    failed_files.append((filename, error_msg))

                # 每个文件下载后暂停1秒，避免请求过于频繁
                time.sleep(1)

            # 显示失败的文件信息
            if failed_files:
                print("\n以下文件处理失败：")
                for filename, error in failed_files:
                    print(f"- {filename}: {error}")
                print("\n您可以稍后重新运行程序尝试下载这些失败的文件")

            print(f"\n下载总结:")
            print(f"- 目标文件总数: {total_files}")
            print(f"- 已处理文件数: {len(downloaded_files)}")
            print(f"- 失败文件数: {len(failed_files)}")
            print(f"- 剩余文件数: {total_files - len(downloaded_files)}")
            print(f"- 数据保存位置: {os.path.abspath(output_file)}")
            print(f"- 下载记录文件: {os.path.abspath(download_record_file)}")

            # 打开保存的文件
            open_file(output_file)
        else:
            print(f"获取文件列表失败: HTTP {response.status_code}")
    except Exception as e:
        print(f"获取文件列表时出错: {str(e)}")


if __name__ == "__main__":
    # 设置日期范围
    start_date = datetime(2022, 7, 1)
    end_date = datetime(2022, 10, 1)
    download_and_process_data(start_date, end_date)
