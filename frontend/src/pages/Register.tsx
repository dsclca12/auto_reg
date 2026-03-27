import { useState } from 'react'
import { apiFetch } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Play, CheckCircle, XCircle } from 'lucide-react'

export default function Register() {
  const [form, setForm] = useState({
    platform: 'trae',
    email: '',
    password: '',
    count: 1,
    proxy: '',
    executor_type: 'protocol',
    captcha_solver: 'yescaptcha',
    mail_provider: 'moemail',
    laoudo_auth: '',
    laoudo_email: '',
    laoudo_account_id: '',
    cfworker_api_url: '',
    cfworker_admin_token: '',
    cfworker_domain: '',
    cfworker_fingerprint: '',
    yescaptcha_key: '',
    solver_url: 'http://localhost:8888',
  })
  const [task, setTask] = useState<any>(null)
  const [polling, setPolling] = useState(false)

  const set = (k: string, v: any) => setForm(f => ({ ...f, [k]: v }))

  const submit = async () => {
    const res = await apiFetch('/tasks/register', {
      method: 'POST',
      body: JSON.stringify({
        platform: form.platform,
        email: form.email || null,
        password: form.password || null,
        count: form.count,
        proxy: form.proxy || null,
        executor_type: form.executor_type,
        captcha_solver: form.captcha_solver,
        extra: {
          mail_provider: form.mail_provider,
          laoudo_auth: form.laoudo_auth,
          laoudo_email: form.laoudo_email,
          laoudo_account_id: form.laoudo_account_id,
          cfworker_api_url: form.cfworker_api_url,
          cfworker_admin_token: form.cfworker_admin_token,
          cfworker_domain: form.cfworker_domain,
          cfworker_fingerprint: form.cfworker_fingerprint,
          yescaptcha_key: form.yescaptcha_key,
          solver_url: form.solver_url,
        },
      }),
    })
    setTask(res)
    setPolling(true)
    pollTask(res.task_id)
  }

  const pollTask = async (id: string) => {
    const interval = setInterval(async () => {
      const t = await apiFetch(`/tasks/${id}`)
      setTask(t)
      if (t.status === 'done' || t.status === 'failed') {
        clearInterval(interval)
        setPolling(false)
        if (t.cashier_urls && t.cashier_urls.length > 0) {
          t.cashier_urls.forEach((url: string) => window.open(url, '_blank'))
        }
      }
    }, 2000)
  }

  return (
    <div className="space-y-6 max-w-2xl animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold text-[var(--text-primary)]">注册任务</h1>
        <p className="text-[var(--text-muted)] text-sm mt-1">创建账号自动注册任务</p>
      </div>

      <Card hoverable>
        <CardHeader><CardTitle>基本配置</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label>平台</Label>
            <Select value={form.platform} onChange={e => set('platform', e.target.value)}>
              <option value="trae">Trae.ai</option>
              <option value="tavily">Tavily</option>
              <option value="cursor">Cursor</option>
              <option value="kiro">Kiro</option>
              <option value="grok">Grok</option>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>执行器</Label>
            <Select value={form.executor_type} onChange={e => set('executor_type', e.target.value)}>
              <option value="protocol">纯协议</option>
              <option value="headless">无头浏览器</option>
              <option value="headed">有头浏览器</option>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>验证码</Label>
            <Select value={form.captcha_solver} onChange={e => set('captcha_solver', e.target.value)}>
              <option value="yescaptcha">YesCaptcha</option>
              <option value="local_solver">本地Solver(Camoufox)</option>
              <option value="manual">手动</option>
            </Select>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>批量数量</Label>
              <Input type="number" value={form.count} onChange={e => set('count', Number(e.target.value))} />
            </div>
            <div className="space-y-2">
              <Label>代理 (可选)</Label>
              <Input type="text" value={form.proxy} onChange={e => set('proxy', e.target.value)} placeholder="http://user:pass@host:port" />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card hoverable>
        <CardHeader><CardTitle>邮箱配置</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label>邮箱服务</Label>
            <Select value={form.mail_provider} onChange={e => set('mail_provider', e.target.value)}>
              <option value="moemail">MoeMail (sall.cc)</option>
              <option value="laoudo">Laoudo</option>
              <option value="cfworker">CF Worker</option>
            </Select>
          </div>
          {form.mail_provider === 'laoudo' && (
            <div className="space-y-3">
              <div className="space-y-2">
                <Label>邮箱地址</Label>
                <Input type="text" value={form.laoudo_email} onChange={e => set('laoudo_email', e.target.value)} placeholder="xxx@laoudo.com" />
              </div>
              <div className="space-y-2">
                <Label>Account ID</Label>
                <Input type="text" value={form.laoudo_account_id} onChange={e => set('laoudo_account_id', e.target.value)} placeholder="563" />
              </div>
              <div className="space-y-2">
                <Label>JWT Token</Label>
                <Input type="text" value={form.laoudo_auth} onChange={e => set('laoudo_auth', e.target.value)} placeholder="eyJ..." />
              </div>
            </div>
          )}
          {form.mail_provider === 'cfworker' && (
            <div className="space-y-3">
              <div className="space-y-2">
                <Label>API URL</Label>
                <Input type="text" value={form.cfworker_api_url} onChange={e => set('cfworker_api_url', e.target.value)} placeholder="https://apimail.example.com" />
              </div>
              <div className="space-y-2">
                <Label>Admin Token</Label>
                <Input type="text" value={form.cfworker_admin_token} onChange={e => set('cfworker_admin_token', e.target.value)} placeholder="abc123,,,abc" />
              </div>
              <div className="space-y-2">
                <Label>域名</Label>
                <Input type="text" value={form.cfworker_domain} onChange={e => set('cfworker_domain', e.target.value)} placeholder="example.com" />
              </div>
              <div className="space-y-2">
                <Label>Fingerprint (可选)</Label>
                <Input type="text" value={form.cfworker_fingerprint} onChange={e => set('cfworker_fingerprint', e.target.value)} placeholder="cfb82279f..." />
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {form.captcha_solver === 'yescaptcha' && (
        <Card hoverable>
          <CardHeader><CardTitle>验证码配置</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-2">
              <Label>YesCaptcha Key</Label>
              <Input type="text" value={form.yescaptcha_key} onChange={e => set('yescaptcha_key', e.target.value)} />
            </div>
          </CardContent>
        </Card>
      )}
      {form.captcha_solver === 'local_solver' && (
        <Card hoverable>
          <CardHeader><CardTitle>本地 Solver 配置</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-2">
              <Label>Solver URL</Label>
              <Input type="text" value={form.solver_url} onChange={e => set('solver_url', e.target.value)} />
            </div>
            <p className="text-xs text-[var(--text-muted)]">启动命令: python services/turnstile_solver/start.py --headless --browser-type camoufox</p>
          </CardContent>
        </Card>
      )}

      <Button onClick={submit} loading={polling} className="w-full">
        <Play className="h-4 w-4" />
        {polling ? '注册中...' : '开始注册'}
      </Button>

      {task && (
        <Card hoverable>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              任务状态
              <Badge variant={
                task.status === 'done' ? 'success' :
                task.status === 'failed' ? 'danger' : 'default'
              }>{task.status}</Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <div className="flex justify-between text-[var(--text-muted)]">
              <span>任务 ID</span><span className="font-mono">{task.id}</span>
            </div>
            <div className="flex justify-between text-[var(--text-muted)]">
              <span>进度</span><span>{task.progress}</span>
            </div>
            {task.success != null && (
              <div className="flex items-center gap-2 text-emerald-400">
                <CheckCircle className="h-4 w-4" />
                成功 {task.success} 个
              </div>
            )}
            {task.errors?.length > 0 && (
              <div className="space-y-1">
                {task.errors.map((e: string, i: number) => (
                  <div key={i} className="flex items-center gap-2 text-red-400">
                    <XCircle className="h-4 w-4" />
                    <span className="text-xs">{e}</span>
                  </div>
                ))}
              </div>
            )}
            {task.error && (
              <div className="flex items-center gap-2 text-red-400">
                <XCircle className="h-4 w-4" />
                <span className="text-xs">{task.error}</span>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
