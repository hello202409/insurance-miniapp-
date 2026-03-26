#!/usr/bin/env python3
"""
新华人寿产品数据抓取脚本
正确处理页面状态筛选和分页机制
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

class NewChinaLifeSpider(BaseSpider):
    """新华人寿爬虫"""
    
    def __init__(self):
        super().__init__(
            company_name='新华人寿',
            base_url='https://www.newchinalife.com/node/372'
        )
    
    def scrape(self):
        """抓取新华人寿产品数据"""
        print("=" * 80)
        print("开始抓取新华人寿产品数据...")
        print("重要: 需要先选择'在售'状态,再进行分页")
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
                
                # 步骤2: 选择"在售"状态
                print("\n步骤2: 选择'在售'状态筛选...")
                try:
                    # 查找"在售"筛选按钮
                    # 尝试多种可能的选择器
                    sale_button = None
                    selectors = [
                        "//button[contains(text(),'在售')]",
                        "//a[contains(text(),'在售')]",
                        "//span[contains(text(),'在售')]",
                        "//li[contains(text(),'在售')]",
                    ]
                    
                    for selector in selectors:
                        try:
                            elements = self.driver.find_elements(By.XPATH, selector)
                            if elements:
                                sale_button = elements[0]
                                break
                        except:
                            continue
                    
                    if sale_button:
                        sale_button.click()
                        print("✓ 已点击'在售'筛选按钮")
                        time.sleep(3)  # 等待页面刷新
                    else:
                        print("⚠ 未找到'在售'筛选按钮,继续抓取...")
                
                except Exception as e:
                    print(f"⚠ 选择'在售'状态失败: {e}")
                    print("继续抓取数据...")
                    time.sleep(2)
                
                # 步骤3: 获取总页数
                print("\n步骤3: 获取分页信息...")
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # 查找分页元素
                pagination = soup.find('div', class_='pagination')
                if not pagination:
                    pagination = soup.find('ul', class_='pagination')
                if not pagination:
                    pagination = soup.find('nav', class_='pagination')
                
                total_pages = 1
                if pagination:
                    # 查找所有页码链接
                    page_links = pagination.find_all('a')
                    page_numbers = []
                    for link in page_links:
                        text = link.get_text(strip=True)
                        if text.isdigit():
                            page_numbers.append(int(text))
                    
                    if page_numbers:
                        total_pages = max(page_numbers)
                        print(f"✓ 检测到 {total_pages} 页")
                    else:
                        print("⚠ 未检测到页码,假设只有1页")
                else:
                    print("⚠ 未检测到分页元素,假设只有1页")
                
                # 步骤4: 遍历所有页
                print(f"\n步骤4: 开始逐页抓取 ({total_pages} 页)...")
                products = []
                
                for page in range(1, total_pages + 1):
                    print(f"\n正在抓取第 {page}/{total_pages} 页...")
                    
                    # 如果不是第一页,点击对应的页码
                    if page > 1:
                        try:
                            # 尝试多种方式点击页码
                            page_clicked = False
                            
                            # 方式1: 通过页码数字点击
                            try:
                                page_link = self.driver.find_element(By.XPATH, f"//a[contains(text(),'{page}')]")
                                page_link.click()
                                page_clicked = True
                            except:
                                pass
                            
                            # 方式2: 通过data-page属性点击
                            if not page_clicked:
                                try:
                                    page_link = self.driver.find_element(By.XPATH, f"//a[@data-page='{page}']")
                                    page_link.click()
                                    page_clicked = True
                                except:
                                    pass
                            
                            # 方式3: 通过页码链接点击
                            if not page_clicked:
                                try:
                                    page_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '_')]")
                                    for link in page_links:
                                        href = link.get_attribute('href')
                                        if f'_{page}' in href:
                                            link.click()
                                            page_clicked = True
                                            break
                                except:
                                    pass
                            
                            if page_clicked:
                                time.sleep(2)  # 等待页面加载
                            else:
                                print(f"  ⚠ 无法跳转到第{page}页")
                                continue
                        
                        except Exception as e:
                            print(f"  ⚠ 跳转第{page}页失败: {e}")
                            continue
                    
                    # 提取当前页的产品
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 从表格中提取
                    page_products = self.extract_products_from_table(html)
                    if not page_products:
                        # 从列表中提取
                        page_products = self.extract_products_from_list(html)
                    
                    # 规范化数据
                    normalized_page_products = []
                    for product in page_products:
                        normalized = self.normalize_product_data(product)
                        if normalized:
                            normalized_page_products.append(normalized)
                    
                    print(f"  ✓ 第 {page} 页抓取到 {len(normalized_page_products)} 个产品")
                    products.extend(normalized_page_products)
                    
                    # 避免请求过快
                    time.sleep(1)
                
                # 去重
                print(f"\n正在去重...")
                unique_products = []
                seen = set()
                for product in products:
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
    spider = NewChinaLifeSpider()
    
    # 抓取数据
    products = spider.scrape()
    
    # 保存数据
    if products:
        output_file = "data/newchinalife_products.xlsx"
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
