#!/usr/bin/env python3
"""
泰康人寿产品数据抓取脚本
"""

import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.base_spider import BaseSpider
from bs4 import BeautifulSoup

class TaikangSpider(BaseSpider):
    """泰康人寿爬虫"""
    
    def __init__(self):
        super().__init__(
            company_name='泰康人寿',
            base_url='https://www.taikanglife.com/publicinfonew/basicnew/pubproduct/commonprodinfonew/list_542_1.html'
        )
    
    def scrape(self):
        """抓取泰康人寿产品数据"""
        print("=" * 80)
        print("开始抓取泰康人寿产品数据...")
        print("=" * 80)
        print()
        
        try:
            with self:
                # 获取首页内容
                print("正在访问首页...")
                html = self.get_page_content(self.base_url)
                
                if not html:
                    print("访问首页失败")
                    return []
                
                # 解析页面
                soup = BeautifulSoup(html, 'html.parser')
                
                # 泰康人寿的产品通常在表格中
                products = []
                
                # 尝试从表格中提取
                table_products = self.extract_products_from_table(html)
                if table_products:
                    print(f"从表格中提取到 {len(table_products)} 条产品")
                    products.extend(table_products)
                
                # 尝试从列表中提取
                list_products = self.extract_products_from_list(html)
                if list_products:
                    print(f"从列表中提取到 {len(list_products)} 条产品")
                    products.extend(list_products)
                
                # 规范化数据
                print(f"\n正在规范化数据...")
                normalized_products = []
                for product in products:
                    normalized = self.normalize_product_data(product)
                    if normalized:
                        normalized_products.append(normalized)
                
                print(f"规范化后: {len(normalized_products)} 条")
                
                # 去重
                unique_products = []
                seen = set()
                for product in normalized_products:
                    key = product.get('product_name', '')
                    if key and key not in seen:
                        seen.add(key)
                        unique_products.append(product)
                
                print(f"去重后: {len(unique_products)} 条")
                
                self.products = unique_products
                
                return self.products
                
        except Exception as e:
            print(f"抓取失败: {e}")
            import traceback
            traceback.print_exc()
            return []

def main():
    """主函数"""
    spider = TaikangSpider()
    
    # 抓取数据
    products = spider.scrape()
    
    # 保存数据
    if products:
        output_file = "data/taikang_products.xlsx"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        spider.save_to_excel(output_file)
        
        # 打印统计信息
        print("\n" + "=" * 80)
        print("抓取完成!")
        print("=" * 80)
        print(f"公司: {spider.company_name}")
        print(f"产品数量: {len(products)}")
        print("=" * 80)
        
        # 显示前10个产品
        print("\n前10个产品:")
        for i, product in enumerate(products[:10], 1):
            print(f"{i}. {product.get('product_name', 'N/A')}")
    else:
        print("未获取到任何产品数据")

if __name__ == "__main__":
    main()
