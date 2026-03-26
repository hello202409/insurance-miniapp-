#!/usr/bin/env python3
"""
NFRA页面增量监测脚本 (完整版) - 支持抓取最后2页
每次抓取最后2页的数据，避免遗漏新数据
"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import hashlib
import json
import os
import sys


class NFRAIncrementalMonitor:
    """NFRA页面增量监测器"""
    
    def __init__(self):
        self.base_url = "https://www.nfra.gov.cn/cn/view/pages/zaixianfuwu/productList.html?orgid=341065&classid=1"
        self.total_pages = 25  # 总页数
        self.pages_to_scrape = 2  # 抓取最后2页
        self.data_dir = "/Users/rocky/Desktop/buddy/data/nfra"
        self.history_file = os.path.join(self.data_dir, "nfra_history.json")
        self.state_file = os.path.join(self.data_dir, "nfra_state.json")
        
        os.makedirs(self.data_dir, exist_ok=True)
    
    def get_page_url(self, page_num):
        """获取指定页面的URL"""
        # URL格式: ...productList.html?orgid=341065&classid=1&page=1
        return f"{self.base_url}&page={page_num}"
    
    async def fetch_page(self, url):
        """获取页面内容"""
        async with async_playwright() as p:
            try:
                print(f"  正在访问: {url}")
                
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                print(f"  ✓ 页面已加载")
                
                # 等待Angular.js渲染
                await asyncio.sleep(8)
                
                # 滚动页面
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                
                html = await page.content()
                await browser.close()
                
                print(f"  ✓ 页面内容获取成功")
                return html
                
            except Exception as e:
                print(f"  ✗ 获取页面失败: {e}")
                return None
    
    def extract_products(self, html, page_num):
        """从HTML中提取产品数据"""
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # 查找表格
        table = soup.find('table', width="100%")
        if not table:
            table = soup.find('table')
        
        if table:
            rows = table.find_all('tr')
            print(f"    找到表格，共 {len(rows)} 行")
            
            for i, row in enumerate(rows):
                # 检查是否是"无搜索结果"行
                row_text = row.get_text(strip=True)
                if '无搜索结果' in row_text or not row_text:
                    continue
                
                # 提取单元格
                cells = row.find_all('td')
                
                # 每行有2列，每列是一个独立的产品
                for j, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if text and len(text) > 3:
                        product = {
                            '提取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            '产品名称': text,
                            '列号': j + 1,
                            '行号': i,
                            '页码': page_num,
                        }
                        
                        # 检查是否有链接
                        link = cell.find('a')
                        if link and link.get('href'):
                            product['链接'] = link.get('href')
                        
                        # 生成哈希
                        product['hash'] = hashlib.md5(text.encode('utf-8')).hexdigest()
                        
                        products.append(product)
        
        return products
    
    def load_history(self):
        """加载历史数据"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载历史文件失败: {e}")
        return {}
    
    def save_history(self, history):
        """保存历史数据"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            print(f"✓ 历史数据已保存")
        except Exception as e:
            print(f"✗ 保存历史文件失败: {e}")
    
    def load_state(self):
        """加载监测状态"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {'first_run': True, 'last_run': None}
    
    def save_state(self, state):
        """保存监测状态"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            print(f"✓ 状态已保存")
        except Exception as e:
            print(f"✗ 保存状态失败: {e}")
    
    def compare_data(self, current_products, history):
        """比较数据,找出新增的内容"""
        existing_hashes = set()
        for date_data in history.values():
            for item in date_data:
                if isinstance(item, dict) and 'hash' in item:
                    existing_hashes.add(item['hash'])
        
        new_products = []
        for product in current_products:
            data_hash = product.get('hash', '')
            if data_hash and data_hash not in existing_hashes:
                new_products.append(product)
                existing_hashes.add(data_hash)
        
        return new_products
    
    def save_to_daily_file(self, products, date_str, suffix=""):
        """保存到每日文件"""
        if not products:
            print("  没有数据需要保存")
            return
        
        try:
            filename = f"nfra_{date_str}{suffix}.xlsx"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(products)
            df.to_excel(filepath, index=False, engine='openpyxl')
            
            print(f"  ✓ 数据已保存到: {filename}")
            print(f"  ✓ 记录数: {len(df)}")
        except Exception as e:
            print(f"  ✗ 保存Excel文件失败: {e}")
    
    def generate_report(self, total_count, new_count, pages_scraped):
        """生成监测报告"""
        date_str = datetime.now().strftime('%Y%m%d')
        report = {
            '监测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '数据源': self.base_url,
            '抓取页数': pages_scraped,
            '抓取页码': f'最后{self.pages_to_scrape}页',
            '当前总记录数': total_count,
            '新增记录数': new_count,
            '监测状态': '成功'
        }
        
        report_file = os.path.join(self.data_dir, f"nfra_report_{date_str}.json")
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
        except:
            pass
        
        return report
    
    async def run(self):
        """执行监测任务"""
        print("=" * 80)
        print(f"NFRA页面增量监测 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # 计算要抓取的页面（最后2页）
        pages_to_fetch = list(range(self.total_pages - self.pages_to_scrape + 1, self.total_pages + 1))
        print(f"\n计划抓取: 第 {min(pages_to_fetch)} 到 {max(pages_to_fetch)} 页（最后{self.pages_to_scrape}页）")
        
        all_products = []
        
        # 逐页抓取
        for page_num in pages_to_fetch:
            print(f"\n--- 正在抓取第 {page_num}/{self.total_pages} 页 ---")
            
            url = self.get_page_url(page_num)
            html = await self.fetch_page(url)
            
            if html:
                products = self.extract_products(html, page_num)
                if products:
                    all_products.extend(products)
                    print(f"    ✓ 本页提取到 {len(products)} 条数据")
                
                # 短暂等待避免请求过快
                await asyncio.sleep(2)
            else:
                print(f"    ✗ 第 {page_num} 页获取失败")
        
        if not all_products:
            print("\n✗ 未提取到任何产品数据")
            return False
        
        print(f"\n✓ 总共提取到 {len(all_products)} 条数据")
        
        # 去重（不同页面可能有重复）
        unique_products = []
        seen_hashes = set()
        for product in all_products:
            if product['hash'] not in seen_hashes:
                unique_products.append(product)
                seen_hashes.add(product['hash'])
        
        print(f"✓ 去重后: {len(unique_products)} 条数据")
        
        # 加载历史状态
        state = self.load_state()
        history = self.load_history()
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        if state['first_run']:
            # 首次运行，保存所有数据
            print("\n【首次运行】保存所有数据...")
            self.save_to_daily_file(unique_products, date_str)
            
            # 保存到历史
            history[date_str] = unique_products
            self.save_history(history)
            
            # 更新状态
            state['first_run'] = False
            state['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.save_state(state)
            
            print(f"\n✓ 首次监测完成")
            
            # 生成报告
            report = self.generate_report(len(unique_products), len(unique_products), len(pages_to_fetch))
        else:
            # 后续运行，只保存新增的内容
            print("\n【增量监测】比对数据变化...")
            new_products = self.compare_data(unique_products, history)
            
            if new_products:
                self.save_to_daily_file(new_products, date_str, "_new")
                
                history[date_str] = unique_products
                self.save_history(history)
                
                print(f"\n✓ 发现 {len(new_products)} 条新数据")
                
                report = self.generate_report(len(unique_products), len(new_products), len(pages_to_fetch))
            else:
                print("\n✓ 未检测到新数据（页面无变化）")
                
                empty_record = [{
                    '内容': '无新数据',
                    'hash': 'no_new_data',
                    '提取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '说明': f'本次监测了最后{self.pages_to_scrape}页，未发现新增内容'
                }]
                self.save_to_daily_file(empty_record, date_str, "_new")
                
                report = self.generate_report(len(unique_products), 0, len(pages_to_fetch))
            
            state['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.save_state(state)
        
        # 显示报告
        print("\n" + "=" * 80)
        print("监测报告")
        print("=" * 80)
        for key, value in report.items():
            print(f"{key}: {value}")
        print("=" * 80)
        
        return True


async def main():
    """主函数"""
    try:
        monitor = NFRAIncrementalMonitor()
        success = await monitor.run()
        
        if success:
            print("\n✓ 监测任务完成")
            return 0
        else:
            print("\n✗ 监测任务失败")
            return 1
    except Exception as e:
        print(f"\n✗ 程序异常: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
