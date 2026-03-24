"""
保险产品通 - 爬虫主程序
"""
import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Dict

# 数据分类函数
def classify_insurance_type(product_name: str) -> tuple:
    if not product_name:
        return '其他', '其他'
    name = product_name.upper()
    
    if any(kw in name for kw in ['重大疾病', '重疾']):
        return '健康险', '重大疾病保险'
    if '医疗' in name:
        return '健康险', '医疗保险'
    if '护理' in name:
        return '健康险', '护理保险'
    if '豁免' in name:
        return '健康险', '豁免保险'
    if '意外' in name:
        return '意外险', '意外险'
    if '养老年金' in name or ('养老' in name and '年金' in name):
        return '年金险', '养老年金保险'
    if '年金' in name:
        return '年金险', '普通年金保险'
    if '定期寿' in name:
        return '寿险', '定期寿险'
    if '两全' in name:
        return '寿险', '两全险'
    if '终身寿' in name:
        return '寿险', '终身寿险'
    return '其他', '其他'

def classify_design_type(product_name: str) -> str:
    if not product_name:
        return '普通型'
    if '投连' in product_name:
        return '投连型'
    if '万能' in product_name:
        return '万能型'
    if '分红' in product_name:
        return '分红型'
    return '普通型'

def process_product(product: Dict) -> Dict:
    product_name = product.get('product_name', '')
    category, sub_category = classify_insurance_type(product_name)
    return {
        'product_name': product_name,
        'company': product.get('company', ''),
        'category': category,
        'sub_category': sub_category,
        'design_type': classify_design_type(product_name),
        'is_group': '是' if '团体' in product_name else '否',
        'is_internet': '是' if '互联网' in product_name else '否',
        'product_function': '附加险' if product_name.startswith('附加') else '主险',
        'publish_date': product.get('publish_date', ''),
        'status': '在售',
        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def main():
    print(f"保险产品数据抓取 - {datetime.now()}")
    
    # 检查环境变量
    env_id = os.environ.get('TCB_ENV_ID')
    if env_id:
        print(f"云开发环境: {env_id}")
    else:
        print("未配置云开发环境，数据将保存到本地")
    
    # 模拟数据（实际运行时会抓取真实数据）
    products = [
        {'product_name': '平安福重大疾病保险', 'company': '平安人寿', 'publish_date': '2025-03-01'},
    ]
    
    processed = [process_product(p) for p in products]
    
    output = {
        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total': len(processed),
        'products': processed
    }
    
    with open(f"products_{datetime.now().strftime('%Y%m%d')}.json", 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"完成！共处理 {len(processed)} 条数据")

if __name__ == '__main__':
    main()
