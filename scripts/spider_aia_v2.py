#!/usr/bin/env python3
"""
友邦人寿产品数据抓取脚本 v2
针对友邦的特殊结构:
1. 产品以列表形式展示(不是表格)
2. 点击产品名称可展开显示详情链接(条款、说明书等)
3. 两级分页:每5页一组,先翻组再翻组内页
"""

import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.base_spider import BaseSpider
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class AIASpiderV2(BaseSpider):
    """友邦人寿爬虫 v2 - 针对列表展示和两级分页"""
    
    def __init__(self):
        super().__init__(
            company_name='友邦人寿',
            base_url='https://www.aia.com.cn/zh-cn/gongkaixinxipilou/jibenxinxi/chanpinjibenxinxi/zaishouchanpin.chanpinjibenxinxi'
        )
    
    def scrape(self):
        """抓取友邦人寿产品数据"""
        print("=" * 80)
        print("开始抓取友邦人寿产品数据...")
        print("网站结构特点:")
        print("  1. 产品以列表形式展示(不是表格)")
        print("  2. 点击产品名称可展开显示详情链接")
        print("  3. 两级分页:每5页一组")
        print("=" * 80)
        print()
        
        try:
            with self:
                # 步骤1: 访问首页
                print("步骤1: 正在访问首页...")
                self.driver.get(self.base_url)
                
                # 等待页面加载
                time.sleep(3)
                
                # 步骤2: 分析页面结构,找到产品列表容器
                print("\n步骤2: 分析页面结构...")
                html = self.driver.page_source
                
                # 尝试查找产品列表的容器
                # 友邦的产品可能在以下结构中
                possible_selectors = [
                    'ul',                    # 无序列表
                    'ol',                    # 有序列表
                    '[class*="list"]',       # 包含"list"的元素
                    '[class*="product"]',    # 包含"product"的元素
                    '[class*="item"]',       # 包含"item"的元素
                ]
                
                product_container = None
                list_items = []
                
                for selector in possible_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for elem in elements:
                            # 检查这个元素是否包含足够多的子元素(至少10个)
                            children = elem.find_elements(By.XPATH, "./*")
                            if len(children) >= 10:
                                # 检查子元素是否包含"保险"文本
                                test_text = children[0].text
                                if '保险' in test_text or len(children) >= 20:
                                    product_container = elem
                                    list_items = children
                                    print(f"✓ 找到产品列表容器: {selector}")
                                    print(f"  子元素数量: {len(list_items)}")
                                    print(f"  示例文本: {test_text[:100]}...")
                                    break
                        if product_container:
                            break
                    except:
                        continue
                
                if not product_container:
                    print("✗ 未找到产品列表容器")
                    print("\n尝试查找所有列表元素...")
                    all_lists = self.driver.find_elements(By.TAG_NAME, 'li')
                    print(f"找到 {len(all_lists)} 个<li>元素")
                    
                    if all_lists:
                        # 过滤出可能的产品项(包含"保险"且不在导航中)
                        potential_products = []
                        for li in all_lists:
                            text = li.text
                            if '保险' in text and len(text) > 5 and len(text) < 100:
                                # 排除导航项
                                if '友邦保险' not in text and '反保险' not in text and '互联网保险' not in text:
                                    potential_products.append(li)
                        
                        print(f"找到 {len(potential_products)} 个可能的产品项")
                        list_items = potential_products
                
                # 步骤3: 提取当前页的产品
                print("\n步骤3: 提取当前页的产品...")
                products = []
                
                if list_items:
                    for i, item in enumerate(list_items, 1):
                        try:
                            product_name = item.text.strip()
                            
                            # 规范化产品名称
                            if self.is_valid_product(product_name):
                                product = {
                                    'product_name': product_name,
                                    'company': self.company_name,
                                    'status': '在售',
                                    'update_time': self.update_time()
                                }
                                
                                # 尝试点击展开获取详情链接
                                try:
                                    item.click()
                                    time.sleep(0.5)
                                    # 查找展开后的链接
                                    links = item.find_elements(By.TAG_NAME, 'a')
                                    link_texts = [link.text for link in links if link.text]
                                    if link_texts:
                                        product['details'] = '; '.join(link_texts)
                                except:
                                    pass
                                
                                products.append(product)
                                
                                if i <= 5:  # 只显示前5个
                                    print(f"  {i}. {product_name}")
                        except Exception as e:
                            continue
                else:
                    print("✗ 未找到任何产品项")
                    return []
                
                print(f"\n当前页提取到 {len(products)} 个产品")
                
                # 步骤4: 分析分页结构
                print("\n步骤4: 分析分页结构...")
                
                # 查找分页容器
                pagination_selectors = [
                    '[class*="pagination"]',
                    '[class*="pager"]',
                    '[class*="page"]',
                ]
                
                pagination = None
                for selector in pagination_selectors:
                    try:
                        pagination = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if pagination:
                            print(f"✓ 找到分页容器: {selector}")
                            break
                    except:
                        continue
                
                if not pagination:
                    print("⚠ 未找到分页容器,可能只有1页")
                    return self.normalize_products(products)
                
                # 分析两级分页结构
                print("\n步骤5: 分析两级分页...")
                
                # 第一级: 查找组按钮(可能显示为页码组,如1-5, 6-10等)
                group_buttons = pagination.find_elements(By.XPATH, ".//*[contains(text(), '-') or contains(@class, 'group')]")
                
                if group_buttons:
                    print(f"✓ 检测到两级分页结构")
                    print(f"  找到 {len(group_buttons)} 个组按钮")
                    
                    total_groups = len(group_buttons)
                    pages_per_group = 5
                    
                    print(f"  总组数: {total_groups}")
                    print(f"  每组页数: {pages_per_group}")
                else:
                    # 没有明显的两级结构,尝试普通的页码
                    page_buttons = pagination.find_elements(By.XPATH, ".//*[contains(text(), '上一页') or contains(text(), '下一页') or (text() and string-length(text()) <= 3 and number(translate(text(), translate(text(), '0123456789', ''), '')) = text())]")
                    
                    if page_buttons:
                        print(f"✓ 检测到普通分页结构")
                        print(f"  找到 {len(page_buttons)} 个分页按钮")
                        total_groups = 1
                        pages_per_group = len(page_buttons)
                    else:
                        print("⚠ 未检测到分页按钮")
                        return self.normalize_products(products)
                
                # 步骤6: 遍历所有组
                print(f"\n步骤6: 开始遍历所有组和页...")
                
                all_products = products.copy()
                
                if total_groups > 1:
                    # 两级分页
                    for group_idx in range(1, total_groups + 1):
                        print(f"\n--- 第 {group_idx}/{total_groups} 组 ---")
                        
                        # 切换到该组
                        try:
                            # 点击对应的组按钮
                            if group_idx <= len(group_buttons):
                                group_buttons[group_idx - 1].click()
                                time.sleep(2)
                                print(f"✓ 切换到第{group_idx}组")
                        except Exception as e:
                            print(f"⚠ 切换组失败: {e}")
                            continue
                        
                        # 遍历组内的页
                        for page in range(1, pages_per_group + 1):
                            print(f"  正在抓取第 {page}/{pages_per_group} 页...", end=' ')
                            
                            if page > 1:
                                try:
                                    # 点击页码
                                    page_button = self.driver.find_element(
                                        By.XPATH,
                                        f"//{self._get_pagination_tag()}//*[text()='{page}' or text()='{group_idx}-{page}']"
                                    )
                                    page_button.click()
                                    time.sleep(1.5)
                                except:
                                    print(f"跳转失败")
                                    continue
                            
                            # 提取当前页产品
                            page_products = self.extract_products_from_current_page()
                            print(f"找到 {len(page_products)} 个产品")
                            all_products.extend(page_products)
                            
                            time.sleep(0.5)
                else:
                    # 普通单级分页
                    for page in range(2, pages_per_group + 1):
                        print(f"\n正在抓取第 {page}/{pages_per_group} 页...", end=' ')
                        
                        try:
                            page_button = self.driver.find_element(
                                By.XPATH,
                                f"//{self._get_pagination_tag()}//*[text()='{page}']"
                            )
                            page_button.click()
                            time.sleep(1.5)
                            
                            page_products = self.extract_products_from_current_page()
                            print(f"找到 {len(page_products)} 个产品")
                            all_products.extend(page_products)
                            
                            time.sleep(0.5)
                        except:
                            print(f"跳转失败")
                            break
                
                # 步骤7: 规范化和去重
                print(f"\n步骤7: 规范化和去重...")
                normalized_products = self.normalize_products(all_products)
                
                unique_products = self.remove_duplicates(normalized_products)
                
                print(f"原始: {len(all_products)} 条")
                print(f"规范化后: {len(normalized_products)} 条")
                print(f"去重后: {len(unique_products)} 条")
                
                self.products = unique_products
                return unique_products
                
        except Exception as e:
            print(f"抓取失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _get_pagination_tag(self):
        """获取分页容器的标签"""
        # 这里简化处理,实际应该动态获取
        return "nav"
    
    def extract_products_from_current_page(self):
        """从当前页面提取产品"""
        try:
            # 重新获取页面元素
            list_items = self.driver.find_elements(By.TAG_NAME, 'li')
            
            products = []
            for item in list_items:
                try:
                    product_name = item.text.strip()
                    
                    if self.is_valid_product(product_name):
                        product = {
                            'product_name': product_name,
                            'company': self.company_name,
                            'status': '在售',
                            'update_time': self.update_time()
                        }
                        products.append(product)
                except:
                    continue
            
            return products
        except:
            return []
    
    def is_valid_product(self, product_name):
        """判断是否是有效的产品名称"""
        # 基本验证
        if not product_name or len(product_name) < 3:
            return False
        
        # 必须包含"保险"
        if '保险' not in product_name:
            return False
        
        # 排除非产品项
        exclude_patterns = [
            '友邦保险',  # 公司名称
            '反保险',    # 导航项
            '互联网保险', # 导航项
            '短期健康',  # 导航项
            '长期医疗',  # 导航项
            '保险欺诈',  # 导航项
            '保险营销员', # 导航项
        ]
        
        for pattern in exclude_patterns:
            if pattern in product_name:
                return False
        
        return True
    
    def update_time(self):
        """获取当前时间"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def normalize_products(self, products):
        """规范化产品列表"""
        normalized = []
        for product in products:
            normalized_product = self.normalize_product_data(product)
            if normalized_product:
                normalized.append(normalized_product)
        return normalized
    
    def remove_duplicates(self, products):
        """去重"""
        seen = set()
        unique = []
        for product in products:
            key = product.get('product_name', '')
            if key and key not in seen:
                seen.add(key)
                unique.append(product)
        return unique

def main():
    """主函数"""
    spider = AIASpiderV2()
    
    # 抓取数据
    products = spider.scrape()
    
    # 保存数据
    if products:
        output_file = "data/aia_products_v2.xlsx"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        spider.save_to_excel(output_file)
        
        print("\n" + "=" * 80)
        print("抓取完成!")
        print("=" * 80)
        print(f"公司: {spider.company_name}")
        print(f"产品数量: {len(products)}")
        print("=" * 80)
        
        print("\n前20个产品:")
        for i, product in enumerate(products[:20], 1):
            print(f"{i}. {product.get('product_name', 'N/A')}")
    else:
        print("未获取到任何产品数据")

if __name__ == "__main__":
    main()
