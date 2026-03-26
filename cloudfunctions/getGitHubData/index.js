const cloud = require('wx-server-sdk')
const request = require('request-promise')

cloud.init({
  env: cloud.DYNAMIC_CURRENT_ENV
})

exports.main = async (event, context) => {
  const { filename = 'products.xlsx' } = event
  
  // GitHub配置
  const GITHUB_TOKEN = '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'
  const GITHUB_OWNER = 'hello202409'
  const GITHUB_REPO = 'insurance-miniapp-'
  
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/data/${filename}`
  
  try {
    // 调用GitHub API
    const response = await request({
      url: url,
      method: 'GET',
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'User-Agent': 'wx-cloud-function',
        'Accept': 'application/vnd.github.v3+json'
      },
      json: true
    })
    
    // 解析base64内容
    const content = response.content
    const decodedContent = Buffer.from(content, 'base64').toString('binary')
    
    return {
      success: true,
      data: decodedContent,
      sha: response.sha,
      size: response.size,
      filename: filename
    }
    
  } catch (error) {
    console.error('获取GitHub数据失败:', error)
    
    return {
      success: false,
      error: error.message || '获取数据失败'
    }
  }
}
