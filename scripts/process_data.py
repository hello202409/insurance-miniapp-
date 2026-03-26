#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据处理主脚本
1. 读取抓取的数据
2. 生成增量文件
3. 生成完整数据
4. 上传到GitHub
"""
import pandas as pd
from pathlib import Path
import json
import subprocess
import sys
from datetime import datetime

def generate_product_code(company, index):
    """
    生成产品代码
    公司简称 + 序号
    
    映射规则：
    - 平安人寿 → pa
    - 中国人寿 → cl
    - 太平人寿 → tp
    - 新华人寿 → xl
    - 泰康人寿 → tk
    - 太平洋人寿 → tpy
    - 友邦人寿 → aia
    """
    company_mapping = {
        '平安人寿': 'pa',
        '中国人寿': 'cl',
        '太平人寿': 'tp',
        '新华人寿': 'xl',
        '泰康人寿': 'tk',
        '太平洋人寿': 'tpy',
        '友邦人寿': 'aia'
    }
    
    prefix = company_mapping.get(company, 'xx')
    return f"{prefix}-{str(index).zfill(3)}"

def generate_full_data(df):
    """生成完整数据（JSON格式）"""
    print("\n生成完整数据...")
    
    # 按公司分组生成产品代码
    df_sorted = df.sort_values(['company', 'product_name']).reset_index(drop=True)
    
    # 为每个公司的产品生成代码
    current_company = None
    company_index = 0
    products_list = []
    
    for idx, row in df_sorted.iterrows():
        # 切换公司时重置序号
        if row['company'] != current_company:
            current_company = row['company']
            company_index = 1
        
        product_code = generate_product_code(row['company'], company_index)
        company_index += 1
        
        # 处理可能的时间戳类型
        update_time = row.get('update_time', '')
        if pd.isna(update_time) or update_time == '':
            update_time = datetime.now().strftime('%Y-%m-%d')
        elif hasattr(update_time, 'strftime'):
            update_time = update_time.strftime('%Y-%m-%d')
        else:
            update_time = str(update_time)
        
        product_data = {
            '_productCode': product_code,
            'product_name': row['product_name'],
            'company': row['company'],
            'category': row.get('category', ''),
            'sub_category': row.get('sub_category', ''),
            'design_type': row.get('design_type', ''),
            'is_group': row.get('is_group', ''),
            'is_internet': row.get('is_internet', ''),
            'product_function': row.get('product_function', ''),
            'status': row.get('status', '在售'),
            'update_time': update_time
        }
        
        products_list.append(product_data)
    
    # 生成完整数据JSON
    full_data = {
        'version': datetime.now().strftime('%Y-%m-%d-%H:%M:%S'),
        'timestamp': int(datetime.now().timestamp() * 1000),
        'total': len(products_list),
        'products': products_list
    }
    
    # 保存完整数据
    output_dir = Path('data')
    output_dir.mkdir(exist_ok=True)
    
    full_data_file = output_dir / 'products_full.json'
    with open(full_data_file, 'w', encoding='utf-8') as f:
        json.dump(full_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 完整数据已保存: {full_data_file}")
    print(f"✓ 总产品数: {len(products_list)}")
    
    return full_data

def upload_to_github(filename):
    """使用curl上传文件到GitHub"""
    print(f"\n上传文件到GitHub: {filename}")
    
    filepath = Path(f'data/{filename}')
    
    if not filepath.exists():
        print(f"❌ 文件不存在: {filepath}")
        return False
    
    # GitHub API配置
    GITHUB_TOKEN = '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'
    GITHUB_OWNER = 'hello202409'
    GITHUB_REPO = 'insurance-miniapp-'
    
    # 先检查文件是否存在
    check_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/data/{filename}"
    
    check_cmd = [
        'curl', '-s', '-X', 'GET',
        f'https://{GITHUB_TOKEN}@github.com/api/v3/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/data/{filename}'
    ]
    
    try:
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)
        sha = None
        
        if result.returncode == 0:
            try:
                response = json.loads(result.stdout)
                if 'sha' in response:
                    sha = response['sha']
            except:
                pass
        
        # 准备上传数据
        import base64
        with open(filepath, 'rb') as f:
            content = f.read()
        
        content_base64 = base64.b64encode(content).decode('ascii')
        
        # 构建PUT请求的数据
        payload = {
            'message': f'更新{filename} - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            'content': content_base64
        }
        
        if sha:
            payload['sha'] = sha
        
        # 将payload写入临时文件
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(payload, f)
            temp_file = f.name
        
        # 使用curl上传，从文件读取数据
        put_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/data/{filename}"
        
        put_cmd = [
            'curl', '-X', 'PUT',
            '-H', f'Authorization: token {GITHUB_TOKEN}',
            '-H', 'Content-Type: application/json',
            '-d', f'@{temp_file}',
            put_url
        ]
        
        result = subprocess.run(put_cmd, capture_output=True, text=True, timeout=60)
        
        # 删除临时文件
        Path(temp_file).unlink()
        
        if result.returncode == 0:
            print(f"✓ 上传成功: {filename}")
            return True
        else:
            print(f"❌ 上传失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ 上传超时")
        return False
    except Exception as e:
        print(f"❌ 上传出错: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("数据处理主脚本")
    print("=" * 60)
    
    # 1. 读取数据
    print("\n步骤1: 读取数据")
    source_file = Path('products_updated.xlsx')
    
    if not source_file.exists():
        print(f"❌ 数据文件不存在: {source_file}")
        return False
    
    df = pd.read_excel(source_file)
    df = df.fillna('')
    print(f"✓ 读取到 {len(df)} 条记录")
    
    # 2. 生成增量数据
    print("\n步骤2: 生成增量数据")
    try:
        # 调用增量生成脚本
        result = subprocess.run(
            ['python3', 'scripts/generate_delta.py'],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            print(f"❌ 增量生成失败")
            print(result.stderr)
            return False
        
        print(result.stdout)
        print("✓ 增量数据生成完成")
        
    except subprocess.TimeoutExpired:
        print("❌ 增量生成超时")
        return False
    except Exception as e:
        print(f"❌ 增量生成出错: {e}")
        return False
    
    # 3. 生成完整数据
    print("\n步骤3: 生成完整数据")
    full_data = generate_full_data(df)
    
    # 4. 上传到GitHub
    print("\n步骤4: 上传到GitHub")
    
    # 上传完整数据
    upload_to_github('products_full.json')
    
    # 上传增量数据（如果存在）
    if Path('data/products_delta.json').exists():
        upload_to_github('products_delta.json')
    
    # 上传数据记录
    if Path('data/last_data_record.json').exists():
        upload_to_github('last_data_record.json')
    
    print("\n" + "=" * 60)
    print("✓ 数据处理完成！")
    print("=" * 60)
    
    return True

if __name__ == '__main__':
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ 脚本运行出错: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
