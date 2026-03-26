#!/usr/bin/env python3
"""
友邦人寿产品数据抓取脚本
实现特殊两级分页逻辑: 每5页1组,每次翻页是翻一组,再从这组里翻每一页
"""

import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.base_spider import BaseSpider
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class AIASpider(BaseSpider):
    """友邦人寿爬虫"""
    
    def __init__(self):
        super().__init__(
            company_name='友邦人寿',
            base_url='https://www.aia.com.cn/zh-cn/gongkaixinxipilou/jibenxinxi/chanpinjibenxinxi/zaishouchanpin.chanpinjibenxinxi'
        )
    
    def scrape(self):
        """抓取友邦人寿产品数据"""
        print("=" * 80)
        print("开始抓取友邦人寿产品数据...")
        print("特殊处理: 两级分页机制")
        print("  第一级: 每5页为一组")
        print("  第二级: 在每组内遍历所有页")
        print("=" * 80)
        print()
        
        try:
            with self:
                # 获取首页内容
                print("步骤1: 正在访问首页...")
                html = self.get_page_content(self.base_url)
                
                if not html:
                    print("访问首页失败")
                    return []
                
                # 步骤2: 分析分页结构
                print("\n步骤2: 分析分页结构...")
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # 查找分页元素
                pagination = soup.find('div', class_='pagination')
                if not pagination:
                    pagination = soup.find('ul', class_='pagination')
                if not pagination:
                    pagination = soup.find('nav')
                
                # 检测是否是两级分页
                is_two_level = False
                total_groups = 1
                pages_per_group = 5
                
                if pagination:
                    # 查找分页组元素
                    group_elements = pagination.find_all('div', class_='page-group')
                    if not group_elements:
                        # 尝试其他可能的选择器
                        group_elements = pagination.select('.group-pages')
                    
                    if group_elements:
                        is_two_level = True
                        total_groups = len(group_elements)
                        print(f"✓ 检测到两级分页结构")
                        print(f"  总组数: {total_groups}")
                        print(f"  每组页数: {pages_per_group}")
                    else:
                        # 检查是否有页码,如果没有页码说明可能需要先点击
                        page_links = pagination.find_all('a')
                        if not page_links:
                            print("⚠ 未检测到分页元素,尝试点击...")
                            # 尝试点击分页区域
                            try:
                                self.driver.find_element(By.CLASS_NAME, 'pagination').click()
                                time.sleep(2)
                                html = self.driver.page_source
                                soup = BeautifulSoup(html, 'html.parser')
                                pagination = soup.find('div', class_='pagination')
                                if pagination:
                                    page_links = pagination.find_all('a')
                            except:
                                pass
                else:
                    print("⚠ 未检测到分页元素,假设只有1页")
                
                # 步骤3: 遍历所有组和页
                products = []
                
                if is_two_level:
                    # 两级分页逻辑
                    print(f"\n步骤3: 开始两级分页抓取...")
                    
                    for group in range(1, total_groups + 1):
                        print(f"\n--- 第 {group}/{total_groups} 组 ---")
                        
                        # 切换到当前组
                        if group > 1:
                            try:
                                # 查找组按钮
                                group_button = self.driver.find_element(
                                    By.XPATH, 
                                    f"//button[contains(@class, 'group') or contains(@data-group, '{group}')]"
                                )
                                group_button.click()
                                time.sleep(2)
                                print(f"✓ 已切换到第{group}组")
                            except:
                                print(f"⚠ 无法切换到第{group}组,尝试直接抓取")
                                continue
                        
                        # 遍历当前组的所有页
                        for page in range(1, pages_per_group + 1):
                            print(f"  正在抓取第 {page}/{pages_per_group} 页...")
                            
                            # 如果不是第一页,点击页码
                            if page > 1:
                                try:
                                    page_link = self.driver.find_element(
                                        By.XPATH,
                                        f"//a[contains(text(),'{page}') or @data-page='{page}']"
                                    )
                                    page_link.click()
                                    time.sleep(2)
                                except:
                                    print(f"    ⚠ 无法跳转到第{page}页")
                                    continue
                            
                            # 提取当前页的产品
                            html = self.driver.page_source
                            page_products = self.extract_products_from_page(html)
                            
                            print(f"    ✓ 第 {page} 页抓取到 {len(page_products)} 个产品")
                            products.extend(page_products)
                            
                            # 避免请求过快
                            time.sleep(1)
                
                else:
                    # 普通单级分页逻辑
                    print(f"\n步骤3: 普通分页抓取...")
                    
                    # 获取总页数
                    page_links = pagination.find_all('a') if pagination else []
                    page_numbers = []
                    for link in page_links:
                        text = link.get_text(strip=True)
                        if text.isdigit():
                            page_numbers.append(int(text))
                    
                    total_pages = max(page_numbers) if page_numbers else 1
                    print(f"总页数: {total_pages}")
                    
                    for page in range(1, total_pages + 1):
                        print(f"\n正在抓取第 {page}/{total_pages} 页...")
                        
                        if page > 1:
                            try:
                                page_link = self.driver.find_element(
                                    By.XPATH,
                                    f"//a[contains(text(),'{page}') or @data-page='{page}']"
                                )
                                page_link.click()
                                time.sleep(2)
                            except:
                                print(f"  ⚠ 无法跳转到第{page}页")
                                continue
                        
                        # 提取当前页的产品
                        html = self.driver.page_source
                        page_products = self.extract_products_from_page(html)
                        
                        print(f"  ✓ 第 {page} 页抓取到 {len(page_products)} 个产品")
                        products.extend(page_products)
                        
                        time.sleep(1)
                
                # 步骤4: 规范化数据
                print(f"\n步骤4: 规范化数据...")
                normalized_products = []
                for product in products:
                    normalized = self.normalize_product_data(product)
                    if normalized:
                        normalized_products.append(normalized)
                
                print(f"规范化后: {len(normalized_products)} 条")
                
                # 去重
                print(f"\n步骤5: 去重...")
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
    
    def extract_products_from_page(self, html):
        """从页面中提取产品"""
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # 从表格中提取
        table_products = self.extract_products_from_table(html)
        if table_products:
            products.extend(table_products)
        
        # 从列表中提取
        list_products = self.extract_products_from_list(html)
        if list_products:
            products.extend(list_products)
        
        return products

def main():
    """主函数"""
    spider = AIASpider()
    
    # 抓取数据
    products = spider.scrape()
    
    # 保存数据
    if products:
        output_file = "data/aia_products.xlsx"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        spider.save_to_excel(output_file)
        
        # 打印统计信息
        print("\n" + "=" * 80)
        print("抓取完成!")
        print("=" * 80)
        print(f"公司: {spider.company_name}")
        print(f"产品数量: {len(products)}")
        print("=" * 80)
        
        # 显示所有产品(友邦人寿产品较少)
        print("\n所有产品:")
        for i, product in enumerate(products, 1):
            print(f"{i}. {product.get('product_name', 'N/A')}")
    else:
        print("未获取到任何产品数据")

if __name__ == "__main__":
    main()
