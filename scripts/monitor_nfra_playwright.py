#!/usr/bin/env python3
"""
NFRA页面增量监测脚本 (Playwright版本)
使用Playwright处理动态加载的Angular.js页面

依赖: pip install playwright beautifulsoup4 openpyxl
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
    """NFRA页面增量监测器（Playwright版本）"""
    
    def __init__(self):
        self.url = "https://www.nfra.gov.cn/cn/view/pages/zaixianfuwu/productList.html?orgid=341065&classid=1&orgname=中国%3Cfont%20color=%27red%27%3E平%3C/font%3E%3Cfont%20color=%27red%27%3E安%3C/font%3E人寿保险股份有限公司"
        self.data_dir = "/Users/rocky/Desktop/buddy/data/nfra"
        self.history_file = os.path.join(self.data_dir, "nfra_history.json")
        self.state_file = os.path.join(self.data_dir, "nfra_state.json")
        
        # 创建数据目录
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def fetch_page(self):
        """获取页面内容"""
        async with async_playwright() as p:
            try:
                print(f"正在访问页面...")
                
                # 启动浏览器
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # 访问页面
                await page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
                print("  ✓ 页面已加载")
                
                # 等待Angular.js渲染完成
                await asyncio.sleep(8)
                
                # 滚动页面确保所有数据加载
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                
                # 获取页面HTML
                html = await page.content()
                
                # 关闭浏览器
                await browser.close()
                
                print(f"  ✓ 页面内容获取成功")
                return html
                
            except Exception as e:
                print(f"✗ 获取页面失败: {e}")
                import traceback
                traceback.print_exc()
                return None
    
    def extract_products(self, html):
        """从HTML中提取产品数据"""
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        print("\n开始提取产品数据...")
        
        # 保存HTML用于调试
        debug_file = os.path.join(self.data_dir, "page_debug.html")
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"  ✓ 页面HTML已保存到: page_debug.html")
        
        # 查找产品表格
        table = soup.find('table', width="100%")
        if not table:
            table = soup.find('table')
        
        if table:
            rows = table.find_all('tr')
            print(f"  找到表格，共 {len(rows)} 行")
            
            for i, row in enumerate(rows):
                # 跳过表头行
                if i == 0:
                    continue
                
                # 检查是否是"无搜索结果"行
                row_text = row.get_text(strip=True)
                if '无搜索结果' in row_text or row_text == '':
                    continue
                
                # 提取单元格
                cells = row.find_all('td')
                if len(cells) >= 1:
                    product = {
                        '提取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    
                    # 提取所有单元格
                    for j, cell in enumerate(cells):
                        text = cell.get_text(strip=True)
                        if text:
                            if j == 0:
                                product['产品名称'] = text
                            elif j == 1:
                                product['链接'] = text
                            else:
                                product[f'字段{j}'] = text
                    
                    # 生成哈希
                    content_str = product.get('产品名称', '') + product.get('链接', '')
                    product['hash'] = hashlib.md5(content_str.encode('utf-8')).hexdigest()
                    
                    products.append(product)
        
        # 如果表格中没有数据，尝试从链接提取
        if not products:
            print("  表格中未找到数据，尝试从链接提取...")
            
            links = soup.find_all('a', href=True)
            for link in links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # 查找包含"保险"的链接
                if '保险' in text and len(text) > 5 and len(text) < 100:
                    exclude_keywords = ['首页', '在线服务', '查询服务', '返回列表', '打印']
                    if not any(kw in text for kw in exclude_keywords):
                        product = {
                            '提取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            '产品名称': text,
                            '链接': href,
                            'hash': hashlib.md5((text + href).encode('utf-8')).hexdigest(),
                        }
                        products.append(product)
        
        print(f"  ✓ 总共提取到 {len(products)} 条数据")
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
            import traceback
            traceback.print_exc()
    
    def generate_report(self, total_count, new_count):
        """生成监测报告"""
        date_str = datetime.now().strftime('%Y%m%d')
        report = {
            '监测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '数据源': self.url,
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
        
        # 获取页面内容
        html = await self.fetch_page()
        if not html:
            print("\n✗ 无法获取页面内容，监测失败")
            return False
        
        # 提取产品数据
        products = self.extract_products(html)
        
        if not products:
            print("\n✗ 未提取到任何产品数据")
            print("  提示: 检查 page_debug.html 查看页面内容")
            return False
        
        # 加载历史状态
        state = self.load_state()
        history = self.load_history()
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        if state['first_run']:
            # 首次运行，保存所有数据
            print("\n【首次运行】保存所有数据...")
            self.save_to_daily_file(products, date_str)
            
            # 保存到历史
            history[date_str] = products
            self.save_history(history)
            
            # 更新状态
            state['first_run'] = False
            state['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.save_state(state)
            
            print(f"\n✓ 首次监测完成")
            print(f"✓ 总记录数: {len(products)}")
            
            # 生成报告
            report = self.generate_report(len(products), len(products))
        else:
            # 后续运行，只保存新增的内容
            print("\n【增量监测】比对数据变化...")
            new_products = self.compare_data(products, history)
            
            if new_products:
                self.save_to_daily_file(new_products, date_str, "_new")
                
                history[date_str] = products
                self.save_history(history)
                
                print(f"\n✓ 发现 {len(new_products)} 条新数据")
                
                report = self.generate_report(len(products), len(new_products))
            else:
                print("\n✓ 未检测到新数据（页面无变化）")
                
                empty_record = [{
                    '内容': '无新数据',
                    'hash': 'no_new_data',
                    '提取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '说明': '本次监测未发现新增内容'
                }]
                self.save_to_daily_file(empty_record, date_str, "_new")
                
                report = self.generate_report(len(products), 0)
            
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
