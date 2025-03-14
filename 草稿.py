from datetime import datetime, timedelta
import gzip
import io
import re

def parse_file_info(html_content):
    # 使用正则表达式匹配文件信息
    pattern = r'blockchair_bitcoin_transactions_\d{8}\.tsv\.gz\s+\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}\s+\d+'
    matches = re.findall(pattern, html_content)
    
    file_info_list = []
    for match in matches:
        # 解析文件信息
        parts = match.split()
        filename = parts[0]
        date_str = f"{parts[1]} {parts[2]} {parts[3]}"
        size = parts[4]
        
        file_info_list.append({
            'filename': filename,
            'date': date_str,
            'size': size
        })
    
    return file_info_list

def download_and_process_data(start_date, end_date):
    # 创建保存数据的目录
    if not os.path.exists('blockchair_data'):
        os.makedirs('blockchair_data')
    
    # 首先获取网页内容
    url = "https://gz.blockchair.com/bitcoin/transactions/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
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