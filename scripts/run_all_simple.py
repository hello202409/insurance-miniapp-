#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简化的数据获取和处理脚本
使用现有数据products_updated.xlsx，生成统计信息并保存到data/products.xlsx
"""
import pandas as pd
from pathlib import Path
import json

def process_existing_data():
    """处理现有数据"""
    # 读取现有数据
    source_file = 'products_updated.xlsx'
    output_dir = Path('data')
    output_file = output_dir / 'products.xlsx'
    
    # 确保输出目录存在
    output_dir.mkdir(exist_ok=True)
    
    print(f"正在读取 {source_file}...")
    df = pd.read_excel(source_file)
    
    print(f"✓ 成功读取 {len(df)} 条产品记录")
    
    # 数据清洗和标准化
    # 不删除重复记录，因为不同的产品代码代表不同的产品
    # 只删除完全相同的行（所有列都相同）
    original_count = len(df)
    df = df.drop_duplicates(keep='first')
    if len(df) < original_count:
        print(f"✓ 删除了 {original_count - len(df)} 条完全重复的记录")
    
    # 填充空值
    df = df.fillna('')
    
    # 保存到data/products.xlsx
    df.to_excel(output_file, index=False)
    print(f"✓ 数据已保存到 {output_file}")
    
    # 生成统计信息
    print("\n=== 产品统计信息 ===")
    print(f"总产品数: {len(df)}")
    print(f"\n各公司产品分布:")
    if 'company' in df.columns:
        company_stats = df['company'].value_counts().sort_index()
        for company, count in company_stats.items():
            print(f"  {company}: {count} 个产品")
    
    # 保存统计信息到JSON
    stats = {
        'total': len(df),
        'companies': company_stats.to_dict() if 'company' in df.columns else {},
        'update_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    stats_file = output_dir / 'stats.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"✓ 统计信息已保存到 {stats_file}")
    
    # 创建output目录并保存最新数据副本
    output_copy_dir = Path('output')
    output_copy_dir.mkdir(exist_ok=True)
    
    output_copy_file = output_copy_dir / f'products_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    df.to_excel(output_copy_file, index=False)
    print(f"✓ 数据副本已保存到 {output_copy_file}")
    
    # 更新output/products.xlsx为最新
    latest_output = output_copy_dir / 'products.xlsx'
    df.to_excel(latest_output, index=False)
    print(f"✓ 最新数据已保存到 {latest_output}")
    
    return df, stats

if __name__ == '__main__':
    try:
        df, stats = process_existing_data()
        print("\n✓ 数据处理完成！")
        print(f"处理后的产品总数: {stats['total']}")
    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
