// 注册页面 - 重定向到注册任务页面
import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

export default function Register() {
  const navigate = useNavigate()
  
  useEffect(() => {
    navigate('/register-task')
  }, [navigate])
  
  return (
    <div style={{ padding: 24, textAlign: 'center' }}>
      <p>正在跳转到注册任务页面...</p>
    </div>
  )
}
