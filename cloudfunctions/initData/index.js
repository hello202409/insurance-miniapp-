const cloud = require('wx-server-sdk')
const request = require('request-promise')

cloud.init({
  env: cloud.DYNAMIC_CURRENT_ENV
})

const db = cloud.database()

/**
 * 首次导入云函数
 * 从GitHub读取完整数据，导入到云数据库
 */
exports.main = async (event, context) => {
  console.log('开始首次导入...')
  
  // GitHub配置
  const GITHUB_TOKEN = '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'
  const GITHUB_OWNER = 'hello202409'
  const GITHUB_REPO = 'insurance-miniapp-'
  
  try {
    // 1. 从GitHub读取完整数据
    console.log('从GitHub读取完整数据...')
    
    const fullDataUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/data/products_full.json`
    
    const response = await request({
      url: fullDataUrl,
      method: 'GET',
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'User-Agent': 'wx-cloud-function',
        'Accept': 'application/vnd.github.v3+json'
      },
      json: true,
      timeout: 30000
    })
    
    console.log('✓ 成功获取数据文件')
    
    // 解码base64内容
    const content = Buffer.from(response.data.content, 'base64').toString('utf-8')
    const fullData = JSON.parse(content)
    
    console.log(`✓ 解析成功: ${fullData.total} 条产品`)
    
    // 2. 清空现有数据
    console.log('\n清空现有数据...')
    const deleteResult = await db.collection('products').get()
    const oldDocs = deleteResult.data
    
    for (const doc of oldDocs) {
      await db.collection('products').doc(doc._id).remove()
    }
    
    console.log(`✓ 删除了 ${oldDocs.length} 条旧数据`)
    
    // 3. 批量插入新数据
    console.log('\n插入新数据...')
    let successCount = 0
    let failCount = 0
    
    for (const product of fullData.products) {
      try {
        await db.collection('products').add({
          data: {
            ...product,
            createTime: db.serverDate(),
            updateTime: db.serverDate()
          }
        })
        successCount++
      } catch (e) {
        console.error('插入失败:', product._productCode, e)
        failCount++
      }
    }
    
    console.log(`✓ 成功插入 ${successCount} 条数据`)
    if (failCount > 0) {
      console.log(`✗ 失败 ${failCount} 条数据`)
    }
    
    return {
      success: true,
      message: '首次导入完成',
      stats: {
        deleted: oldDocs.length,
        inserted: successCount,
        failed: failCount,
        version: fullData.version
      }
    }
    
  } catch (error) {
    console.error('导入失败:', error)
    
    return {
      success: false,
      message: '导入失败',
      error: error.message
    }
  }
}
