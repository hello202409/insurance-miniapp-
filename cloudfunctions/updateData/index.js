const cloud = require('wx-server-sdk')
const request = require('request-promise')

cloud.init({
  env: cloud.DYNAMIC_CURRENT_ENV
})

const db = cloud.database()

/**
 * 增量更新云函数
 * 从GitHub读取增量文件，更新云数据库
 */
exports.main = async (event, context) => {
  console.log('开始增量更新...')
  
  // GitHub配置
  const GITHUB_TOKEN = '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'
  const GITHUB_OWNER = 'hello202409'
  const GITHUB_REPO = 'insurance-miniapp-'
  
  try {
    // 1. 获取增量文件
    console.log('从GitHub读取增量文件...')
    
    const deltaUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/data/products_delta.json`
    
    const response = await request({
      url: deltaUrl,
      method: 'GET',
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'User-Agent': 'wx-cloud-function',
        'Accept': 'application/vnd.github.v3+json'
      },
      json: true,
      timeout: 30000
    })
    
    console.log('✓ 成功获取增量文件')
    console.log(`版本: ${response.data.version}`)
    console.log(`新增: ${response.data.added_count} 条`)
    console.log(`删除: ${response.data.removed_count} 条`)
    
    const deltaData = response.data
    
    // 2. 处理新增产品
    let addedCount = 0
    if (deltaData.added && deltaData.added.length > 0) {
      console.log(`\n处理新增产品 (${deltaData.added.length} 条)...`)
      
      for (const product of deltaData.added) {
        try {
          // 检查是否已存在
          const existing = await db.collection('products')
            .where({
              product_name: product.product_name,
              company: product.company
            })
            .get()
          
          if (existing.data.length > 0) {
            console.log(`  - 跳过（已存在）: ${product.company} - ${product.product_name}`)
            continue
          }
          
          // 生成产品代码
          const productCode = generateProductCode(product.company, product.product_name)
          
          await db.collection('products').add({
            data: {
              ...product,
              _productCode: productCode,
              createTime: db.serverDate(),
              updateTime: db.serverDate()
            }
          })
          
          addedCount++
          console.log(`  ✓ 新增: ${product.company} - ${product.product_name} (${productCode})`)
          
        } catch (error) {
          console.error(`  ✗ 新增失败: ${product.product_name}`, error)
        }
      }
    }
    
    // 3. 处理删除产品（移到products_deleted）
    let removedCount = 0
    if (deltaData.removed && deltaData.removed.length > 0) {
      console.log(`\n处理删除产品 (${deltaData.removed.length} 条)...`)
      
      for (const product of deltaData.removed) {
        try {
          // 查找要删除的产品
          const existing = await db.collection('products')
            .where({
              product_name: product.product_name,
              company: product.company
            })
            .get()
          
          if (existing.data.length === 0) {
            console.log(`  - 跳过（不存在）: ${product.company} - ${product.product_name}`)
            continue
          }
          
          const doc = existing.data[0]
          
          // 移到products_deleted（留痕）
          await db.collection('products_deleted').add({
            data: {
              ...doc,
              deletedAt: db.serverDate(),
              deleteReason: '停售下架'
            }
          })
          
          // 从products删除
          await db.collection('products').doc(doc._id).remove()
          
          removedCount++
          console.log(`  ✓ 删除: ${product.company} - ${product.product_name} (${doc._productCode})`)
          
        } catch (error) {
          console.error(`  ✗ 删除失败: ${product.product_name}`, error)
        }
      }
    }
    
    console.log('\n' + '=' * 50)
    console.log('增量更新完成')
    console.log('=' * 50)
    console.log(`版本: ${deltaData.version}`)
    console.log(`新增: ${addedCount} 条`)
    console.log(`删除: ${removedCount} 条`)
    console.log('=' * 50)
    
    return {
      success: true,
      message: '增量更新完成',
      version: deltaData.version,
      stats: {
        added: addedCount,
        removed: removedCount,
        timestamp: deltaData.timestamp
      }
    }
    
  } catch (error) {
    console.error('增量更新失败:', error)
    
    return {
      success: false,
      message: '增量更新失败',
      error: error.message
    }
  }
}

/**
 * 生成产品代码
 */
function generateProductCode(company, productName) {
  const companyPrefix = {
    '平安人寿': 'pa',
    '中国人寿': 'cl',
    '太平人寿': 'tp',
    '新华人寿': 'xl',
    '泰康人寿': 'tk',
    '太平洋人寿': 'tpy',
    '友邦人寿': 'aia'
  }
  
  const prefix = companyPrefix[company] || 'xx'
  const suffix = productName.substring(0, 2).replace(/\s/g, '')
  const timestamp = Date.now().toString().slice(-4)
  
  return `${prefix}-${suffix}${timestamp}`
}
