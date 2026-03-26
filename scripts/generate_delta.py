#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增量数据生成脚本
对比前后两次抓取的数据，生成增量文件
"""
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

def get_product_key(row):
    """
    生成产品唯一标识
    使用公司名称+产品名称作为唯一key
    """
    key = f"{row['company']}_{row['product_name']}"
    return key

def load_previous_data():
    """加载上次的数据记录"""
    record_file = Path('data/last_data_record.json')
    if record_file.exists():
        try:
            with open(record_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except (json.JSONDecodeError, KeyError):
            print("历史数据记录文件格式错误，将重新生成")
            return None
    return None

def save_current_record(df):
    """保存当前数据记录"""
    products = []
    for _, row in df.iterrows():
        product = {
            'key': get_product_key(row),
            'company': row['company'],
            'product_name': row['product_name'],
            'category': str(row.get('category', '')),
            'sub_category': str(row.get('sub_category', '')),
            'design_type': str(row.get('design_type', '')),
            'is_group': str(row.get('is_group', '')),
            'is_internet': str(row.get('is_internet', '')),
            'product_function': str(row.get('product_function', '')),
            'status': str(row.get('status', '在售')),
            'update_time': str(row.get('update_time', '')),
        }
        products.append(product)
    
    record = {
        'version': datetime.now().strftime('%Y-%m-%d-%H:%M:%S'),
        'timestamp': int(datetime.now().timestamp() * 1000),
        'products': products
    }
    
    with open('data/last_data_record.json', 'w', encoding='utf-8') as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    
    return record

def generate_delta(df):
    """
    生成增量数据
    """
    print("=" * 50)
    print("开始生成增量数据")
    print("=" * 50)
    
    # 加载上次的数据记录
    previous_record = load_previous_data()
    
    if previous_record is None:
        print("首次运行或历史记录无效")
        print("将生成完整数据，不生成增量文件")
        
        # 保存当前记录
        save_current_record(df)
        
        return None
    
    print(f"上次数据版本: {previous_record.get('version', 'unknown')}")
    print(f"上次数据数量: {len(previous_record.get('products', []))}")
    
    # 生成当前产品字典
    current_products = {}
    for _, row in df.iterrows():
        key = get_product_key(row)
        current_products[key] = {
            'company': row['company'],
            'product_name': row['product_name'],
            'category': str(row.get('category', '')),
            'sub_category': str(row.get('sub_category', '')),
            'design_type': str(row.get('design_type', '')),
            'is_group': str(row.get('is_group', '')),
            'is_internet': str(row.get('is_internet', '')),
            'product_function': str(row.get('product_function', '')),
            'status': str(row.get('status', '在售')),
            'update_time': str(row.get('update_time', '')),
        }
    
    # 生成上次产品字典
    previous_products = {}
    for product in previous_record.get('products', []):
        key = product.get('key', '')
        if key:
            previous_products[key] = product
    
    # 找出新增的产品
    added_products = []
    for key, product in current_products.items():
        if key not in previous_products:
            added_products.append(product)
            print(f"  [新增] {product['company']} - {product['product_name']}")
    
    # 找出删除的产品
    removed_products = []
    for key, product in previous_products.items():
        if key not in current_products:
            removed_products.append(product)
            print(f"  [删除] {product['company']} - {product['product_name']}")
    
    # 如果没有变化，不生成增量文件
    if not added_products and not removed_products:
        print("=" * 50)
        print("数据无变化，不生成增量文件")
        print("=" * 50)
        return None
    
    # 生成增量数据
    delta_data = {
        'version': datetime.now().strftime('%Y-%m-%d-%H:%M:%S'),
        'timestamp': int(datetime.now().timestamp() * 1000),
        'previous_version': previous_record.get('version', 'unknown'),
        'added_count': len(added_products),
        'removed_count': len(removed_products),
        'added': added_products,
        'removed': removed_products
    }
    
    print("=" * 50)
    print(f"增量数据统计:")
    print(f"  新增: {len(added_products)} 条")
    print(f"  删除: {len(removed_products)} 条")
    print(f"  当前版本: {delta_data['version']}")
    print(f"  上次版本: {delta_data['previous_version']}")
    print("=" * 50)
    
    # 保存增量文件
    delta_file = Path('data/products_delta.json')
    with open(delta_file, 'w', encoding='utf-8') as f:
        json.dump(delta_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 增量文件已保存: {delta_file}")
    
    # 更新当前数据记录
    save_current_record(df)
    
    return delta_data

def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("增量数据生成脚本")
    print("=" * 60 + "\n")
    
    # 读取当前数据
    data_file = Path('products_updated.xlsx')
    
    # 如果当前目录没有，尝试上一级目录
    if not data_file.exists():
        data_file = Path('../products_updated.xlsx')
    
    if not data_file.exists():
        print(f"❌ 数据文件不存在: {data_file}")
        return False
    
    print(f"读取数据文件: {data_file}")
    df = pd.read_excel(data_file)
    print(f"✓ 成功读取 {len(df)} 条记录\n")
    
    # 数据清洗
    df = df.fillna('')
    print("✓ 数据清洗完成\n")
    
    # 生成增量数据
    delta_data = generate_delta(df)
    
    if delta_data is None:
        print("\n首次运行或数据无变化")
        return True
    
    print("\n✓ 增量数据生成完成！")
    
    # 显示详细信息
    if delta_data['added_count'] > 0 or delta_data['removed_count'] > 0:
        print("\n数据变化详情:")
        if delta_data['added_count'] > 0:
            print(f"  新增产品 ({delta_data['added_count']} 条):")
            for product in delta_data['added'][:5]:
                print(f"    - {product['company']}: {product['product_name']}")
            if delta_data['added_count'] > 5:
                print(f"    ... 还有 {delta_data['added_count'] - 5} 条")
        
        if delta_data['removed_count'] > 0:
            print(f"\n  删除产品 ({delta_data['removed_count']} 条):")
            for product in delta_data['removed'][:5]:
                print(f"    - {product['company']}: {product['product_name']}")
            if delta_data['removed_count'] > 5:
                print(f"    ... 还有 {delta_data['removed_count'] - 5} 条")
    
    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
