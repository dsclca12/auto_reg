import { useState, useEffect } from 'react'
import { Card, Switch, Input, Button, Space, Tag, Typography, Form, App, Modal, Spin, Alert, InputNumber } from 'antd'
import {
  SaveOutlined,
  ReloadOutlined,
  WalletOutlined,
  GlobalOutlined,
  KeyOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons'
import { apiFetch } from '@/lib/utils'

const { Title, Text, Paragraph } = Typography

/**
 * 贡献设置页面
 * 功能：配置贡献服务器、查看统计信息、兑换余额
 */
export default function ContributionPage() {
  const { message: msg, modal } = App.useApp()
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  // 配置状态
  const [config, setConfig] = useState({
    enabled: false,  // 默认关闭
    api_url: '',
    api_key: '',
  })

  // 服务器统计信息
  const [quotaStats, setQuotaStats] = useState<any>(null)
  // Key 信息
  const [keyInfo, setKeyInfo] = useState<any>(null)
  // 兑换相关
  const [redeemAmount, setRedeemAmount] = useState<number | undefined>(undefined)
  const [redeeming, setRedeeming] = useState(false)

  // 加载配置
  const loadConfig = async () => {
    try {
      const data = await apiFetch('/api/contribution/config')
      setConfig(data)
      form.setFieldsValue(data)
    } catch (error: any) {
      msg.error('加载配置失败: ' + error.message)
    }
  }

  // 加载统计信息
  const loadStats = async () => {
    if (!config.api_url) return
    
    setRefreshing(true)
    try {
      const [statsData, keyData] = await Promise.all([
        apiFetch('/api/contribution/quota-stats'),
        config.api_key ? apiFetch('/api/contribution/key-info') : Promise.resolve(null),
      ])
      setQuotaStats(statsData)
      setKeyInfo(keyData)
    } catch (error: any) {
      msg.warning('加载统计信息失败: ' + error.message)
    } finally {
      setRefreshing(false)
    }
  }

  // 保存配置
  const handleSaveConfig = async (values: any) => {
    setLoading(true)
    try {
      await apiFetch('/api/contribution/config', {
        method: 'POST',
        body: JSON.stringify(values),
      })
      setConfig(values)
      msg.success('配置已保存')
      // 重新加载统计信息
      setTimeout(loadStats, 500)
    } catch (error: any) {
      msg.error('保存失败: ' + error.message)
    } finally {
      setLoading(false)
    }
  }

  // 生成新 Key
  const handleGenerateKey = async () => {
    if (!config.api_url) {
      msg.warning('请先配置服务器地址')
      return
    }

    Modal.confirm({
      title: '生成新的 API Key',
      content: '确定要生成新的 API Key 吗？新 Key 将自动保存到配置中。',
      onOk: async () => {
        try {
          const result = await apiFetch('/api/contribution/generate-key', {
            method: 'POST',
          })
          if (result.key) {
            form.setFieldsValue({ api_key: result.key })
            setConfig(prev => ({ ...prev, api_key: result.key }))
            msg.success('新 Key 已生成并保存')
            setTimeout(loadStats, 500)
          }
        } catch (error: any) {
          msg.error('生成失败: ' + error.message)
        }
      },
    })
  }

  // 兑换余额
  const handleRedeem = async () => {
    if (!config.api_key) {
      msg.warning('请先配置 API Key')
      return
    }

    if (!redeemAmount || redeemAmount <= 0) {
      msg.warning('请输入有效的兑换金额')
      return
    }

    Modal.confirm({
      title: '确认兑换',
      content: `确定要兑换 $${redeemAmount} 吗？`,
      okText: '确认兑换',
      okType: 'danger',
      onOk: async () => {
        setRedeeming(true)
        try {
          const result = await apiFetch('/api/contribution/redeem', {
            method: 'POST',
            body: JSON.stringify({ amount_usd: redeemAmount }),
          })
          
          if (result.code) {
            modal.success({
              title: '兑换成功！',
              content: (
                <div>
                  <p>兑换码：<Text copyable strong>{result.code}</Text></p>
                  <p>兑换金额：${result.redeemed_amount_usd?.toFixed(2)}</p>
                  <p>剩余余额：${result.remaining_balance_usd?.toFixed(2)}</p>
                </div>
              ),
            })
            // 刷新统计信息
            setTimeout(loadStats, 1000)
          }
        } catch (error: any) {
          msg.error('兑换失败: ' + error.message)
        } finally {
          setRedeeming(false)
        }
      },
    })
  }

  // 初始化
  useEffect(() => {
    loadConfig()
  }, [])

  useEffect(() => {
    if (config.api_url) {
      loadStats()
    }
  }, [config.api_url])

  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      {/* 页面标题 */}
      <div style={{ marginBottom: 24 }}>
        <Title level={3} style={{ margin: 0 }}>
          <ThunderboltOutlined style={{ marginRight: 8, color: '#1890ff' }} />
          贡献设置
        </Title>
        <Text type="secondary">
          配置将持久化保存，注册任务自动使用
        </Text>
      </div>

      {/* 配置卡片 */}
      <Card
        title="配置"
        style={{ marginBottom: 16 }}
        extra={
          <Button
            type="primary"
            icon={<SaveOutlined />}
            loading={loading}
            onClick={() => form.submit()}
          >
            保存配置
          </Button>
        }
      >
        {/* 提示信息 */}
        <Alert
          message="开启贡献模式后，注册成功账号将只上传到贡献服务器"
          description={
            <div>
              <p style={{ margin: '4px 0' }}>
                CPA / CodexProxy / Sub2API 自动上传会被停用，避免重复上报。
              </p>
              <p style={{ margin: '4px 0' }}>
                目前该功能在 xem 中转站测试中，有兴趣可以进群了解
              </p>
              <p style={{ margin: '4px 0' }}>
                中转站: https://ai.xem8k5.top/ 群号: 634758974
              </p>
            </div>
          }
          type="warning"
          showIcon
          style={{ marginBottom: 24 }}
        />

        <Form
          form={form}
          layout="vertical"
          onFinish={handleSaveConfig}
          initialValues={config}
        >
          {/* 开关 */}
          <Form.Item label="是否开启" name="enabled" valuePropName="checked">
            <Switch
              checkedChildren="开启"
              unCheckedChildren="关闭"
              style={{ width: 60 }}
            />
          </Form.Item>

          {/* 服务器地址 */}
          <Form.Item
            label={<span><span style={{ color: 'red' }}>*</span> 服务器地址</span>}
            name="api_url"
            rules={[{ required: true, message: '请输入服务器地址' }]}
          >
            <Input
              placeholder="http://new.xem8k5.top:7317/"
              prefix={<GlobalOutlined />}
            />
          </Form.Item>

          {/* API Key */}
          <Form.Item
            label="API Key"
            name="api_key"
            extra={
              <Button
                type="link"
                size="small"
                onClick={handleGenerateKey}
                style={{ padding: 0 }}
              >
                没有 key？请求新建
              </Button>
            }
          >
            <Input.Password
              placeholder="请输入 API Key"
              prefix={<KeyOutlined />}
            />
          </Form.Item>
        </Form>
      </Card>

      {/* 信息卡片 */}
      <Card
        title="信息"
        style={{ marginBottom: 16 }}
        extra={
          <Button
            icon={<ReloadOutlined spin={refreshing} />}
            onClick={loadStats}
            loading={refreshing}
            disabled={!config.api_url}
          >
            刷新信息
          </Button>
        }
      >
        {refreshing && !quotaStats ? (
          <div style={{ textAlign: 'center', padding: 20 }}>
            <Spin tip="加载中..." />
          </div>
        ) : quotaStats ? (
          <>
            {/* 服务器信息 */}
            <div style={{ marginBottom: 16 }}>
              <Title level={5}>服务器信息</Title>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                {quotaStats.quota_account_count !== undefined && (
                  <Tag color="blue">账号数: {quotaStats.quota_account_count}</Tag>
                )}
                {quotaStats.quota_total !== undefined && (
                  <Tag color="blue">总额度: {quotaStats.quota_total.toFixed(3)}</Tag>
                )}
                {quotaStats.quota_used !== undefined && (
                  <Tag color="orange">已用额度: {quotaStats.quota_used.toFixed(3)}</Tag>
                )}
                {quotaStats.quota_remaining !== undefined && (
                  <Tag color="green">剩余额度: {quotaStats.quota_remaining.toFixed(3)}</Tag>
                )}
                {quotaStats.quota_used_percent !== undefined && (
                  <Tag color="orange">已用占比: {quotaStats.quota_used_percent.toFixed(2)}%</Tag>
                )}
                {quotaStats.quota_remaining_percent !== undefined && (
                  <Tag color="green">剩余占比: {quotaStats.quota_remaining_percent.toFixed(2)}%</Tag>
                )}
                {quotaStats.quota_remaining_accounts !== undefined && (
                  <Tag color="purple">
                    剩余额度折算账号数: {quotaStats.quota_remaining_accounts.toFixed(2)}
                  </Tag>
                )}
              </div>

              {config.api_key && (
                <div style={{ marginTop: 8 }}>
                  <Text strong>API Key</Text>
                  <Text copyable style={{ marginLeft: 8 }}>
                    {config.api_key}
                  </Text>
                </div>
              )}
            </div>

            {/* Key 信息 */}
            {keyInfo && (
              <div style={{ marginTop: 16 }}>
                <Title level={5}>Key 信息</Title>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                  {keyInfo.balance_usd !== undefined && (
                    <Tag color="blue">余额: {keyInfo.balance_usd}</Tag>
                  )}
                  {keyInfo.source && (
                    <Tag color="cyan">来源: {keyInfo.source}</Tag>
                  )}
                  {keyInfo.bound_account_count !== undefined && (
                    <Tag color="green">绑定账号数: {keyInfo.bound_account_count}</Tag>
                  )}
                  {keyInfo.settled_amount_usd !== undefined && (
                    <Tag color="purple">结算金额: {keyInfo.settled_amount_usd}</Tag>
                  )}
                </div>
              </div>
            )}
          </>
        ) : (
          <Text type="secondary">
            {config.api_url ? '暂无数据' : '请先配置服务器地址'}
          </Text>
        )}
      </Card>

      {/* 提现卡片 */}
      <Card title="提现" style={{ marginBottom: 16 }}>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {keyInfo?.balance_usd !== undefined && (
            <div>
              <Text>Key 当前额度：</Text>
              <Text strong style={{ color: '#1890ff' }}>
                ${keyInfo.balance_usd}
              </Text>
            </div>
          )}

          <div>
            <div style={{ marginBottom: 8 }}>提现金额</div>
            <InputNumber
              style={{ width: 200 }}
              placeholder="请输入金额（美元）"
              value={redeemAmount}
              onChange={(val) => setRedeemAmount(val || undefined)}
              min={0}
              precision={2}
              prefix="$"
            />
          </div>

          <Button
            type="primary"
            danger
            loading={redeeming}
            disabled={!redeemAmount || redeemAmount <= 0}
            icon={<WalletOutlined />}
            onClick={handleRedeem}
          >
            提现确认
          </Button>
        </Space>
      </Card>

      {/* 底部说明 */}
      <Card>
        <Title level={5}>使用说明</Title>
        <Paragraph>
          <ul style={{ paddingLeft: 20 }}>
            <li>开启贡献模式后，注册成功的账号将自动上传到配置的贡献服务器</li>
            <li>支持查看服务器配额统计和 API Key 信息</li>
            <li>可以将余额兑换成兑换码，方便转移或分享</li>
            <li>所有配置将自动保存，重启后无需重新配置</li>
          </ul>
        </Paragraph>
      </Card>
    </div>
  )
}
