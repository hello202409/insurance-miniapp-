#!/usr/bin/env python3
"""
运行所有保险公司的产品数据抓取
"""

import os
import sys
import time
from datetime import datetime
import pandas as pd

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入分类器
from utils.classifier import ProductClassifier

class ProductScraper:
    """产品抓取管理器"""
    
    def __init__(self):
        self.classifier = ProductClassifier()
        self.output_dir = 'data'
        
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
    
    def run_all_scrapers(self):
        """运行所有抓取脚本"""
        print("=" * 80)
        print("开始抓取所有保险公司的产品数据")
        print("=" * 80)
        print()
        
        scrapers = [
            ('平安人寿', 'spider_pingan.py'),
            ('中国人寿', 'spider_chinalife.py'),
            ('太平洋人寿', 'spider_cpic.py'),
            ('太平人寿', 'spider_taiping.py'),
            ('新华人寿', 'spider_newchinalife.py'),
            ('泰康人寿', 'spider_taikang.py'),
            ('友邦人寿', 'spider_aia.py'),
        ]
        
        all_products = []
        
        for company, script_name in scrapers:
            print(f"\n正在抓取 {company}...")
            print("-" * 80)
            
            try:
                # 运行抓取脚本
                script_path = os.path.join('scripts', script_name)
                
                if os.path.exists(script_path):
                    # 导入并运行脚本
                    module_name = script_name.replace('.py', '')
                    exec(open(script_path).read(), globals())
                    
                    print(f"✓ {company} 抓取完成")
                else:
                    print(f"⚠ {script_name} 不存在,跳过")
                    continue
                
                # 等待避免请求过快
                time.sleep(2)
                
            except Exception as e:
                print(f"✗ {company} 抓取失败: {e}")
                continue
        
        print()
        print("=" * 80)
        print("所有抓取任务完成")
        print("=" * 80)
    
    def merge_all_data(self):
        """合并所有抓取的数据"""
        print("\n开始合并数据...")
        print("-" * 80)
        
        all_data = []
        
        # 读取所有数据文件
        for filename in os.listdir(self.output_dir):
            if filename.endswith('.xlsx') and filename != 'products.xlsx':
                file_path = os.path.join(self.output_dir, filename)
                
                try:
                    df = pd.read_excel(file_path)
                    all_data.append(df)
                    print(f"✓ 读取 {filename}: {len(df)} 条")
                except Exception as e:
                    print(f"✗ 读取 {filename} 失败: {e}")
        
        if all_data:
            # 合并所有数据
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # 去重
            print(f"\n合并前: {len(merged_df)} 条")
            merged_df = merged_df.drop_duplicates(subset=['product_name', 'company'], keep='first')
            print(f"去重后: {len(merged_df)} 条")
            
            # 保存合并后的数据
            output_path = os.path.join(self.output_dir, 'products.xlsx')
            merged_df.to_excel(output_path, index=False)
            
            print(f"\n✓ 数据已保存到: {output_path}")
            return merged_df
        else:
            print("⚠ 没有找到可合并的数据")
            return None
    
    def classify_all_products(self, df):
        """对所有产品进行分类"""
        print("\n开始产品分类...")
        print("-" * 80)
        
        if df is None:
            print("⚠ 没有数据可分类")
            return None
        
        # 使用分类器
        classified_df = self.classifier.classify_dataframe(df)
        
        print(f"✓ 产品分类完成: {len(classified_df)} 条")
        
        # 保存分类后的数据
        output_path = os.path.join(self.output_dir, 'products_classified.xlsx')
        classified_df.to_excel(output_path, index=False)
        
        print(f"✓ 分类后的数据已保存到: {output_path}")
        
        # 显示统计信息
        print("\n分类统计:")
        print("-" * 80)
        print("各公司产品数量:")
        print(classified_df['company'].value_counts().sort_values(ascending=False))
        print()
        print("各险种大类分布:")
        print(classified_df['category'].value_counts().sort_values(ascending=False))
        
        return classified_df

def main():
    """主函数"""
    scraper = ProductScraper()
    
    # 1. 运行所有抓取脚本
    scraper.run_all_scrapers()
    
    # 2. 合并数据
    merged_data = scraper.merge_all_data()
    
    # 3. 产品分类
    classified_data = scraper.classify_all_products(merged_data)
    
    if classified_data is not None:
        print("\n" + "=" * 80)
        print("所有任务完成!")
        print(f"总产品数量: {len(classified_data)}")
        print("=" * 80)

if __name__ == "__main__":
    main()
