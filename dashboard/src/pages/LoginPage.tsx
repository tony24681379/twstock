/**
 * 登入頁面
 *
 * 使用 Google One Tap 登入（無需 redirect URI）
 */

import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { GoogleLogin, CredentialResponse } from '@react-oauth/google'
import { useAuth } from '../contexts/AuthContext'

export function LoginPage() {
  const { isAuthenticated, login } = useAuth()
  const navigate = useNavigate()

  useEffect(() => {
    // 如果已登入，重導向到首頁
    if (isAuthenticated) {
      navigate('/')
    }
  }, [isAuthenticated, navigate])

  const handleSuccess = async (credentialResponse: CredentialResponse) => {
    try {
      if (!credentialResponse.credential) {
        throw new Error('Google 未回傳登入憑證')
      }

      console.log('Received credential, processing login...')

      // 使用 Google 提供的 ID token
      await login(credentialResponse.credential)

      console.log('Login successful, redirecting...')

      // 登入成功後重導向到首頁
      navigate('/')
    } catch (error) {
      console.error('Login error:', error)
      const errorMessage = error instanceof Error
        ? error.message
        : '登入失敗，請稍後再試'

      alert(`登入失敗\n\n錯誤訊息：${errorMessage}\n\n請確認：\n1. 網路連線正常\n2. 使用的是有效的 Google 帳號\n3. 瀏覽器允許第三方 cookies`)
    }
  }

  const handleError = () => {
    alert('Google 登入失敗')
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 px-4">
      <div className="max-w-md w-full space-y-8">
        <div>
          <h2 className="mt-6 text-center text-3xl font-extrabold text-gray-900 dark:text-gray-100">
            台股籌碼集中度儀表板
          </h2>
          <p className="mt-2 text-center text-sm text-gray-600 dark:text-gray-400">
            請使用 Google 帳號登入以存取儀表板
          </p>
        </div>

        <div className="mt-8 space-y-6">
          <div className="flex justify-center">
            <GoogleLogin
              onSuccess={handleSuccess}
              onError={handleError}
              useOneTap
              text="signin_with"
              size="large"
              width="400"
              auto_select={false}
              cancel_on_tap_outside={false}
            />
          </div>

          <p className="text-xs text-center text-gray-500 dark:text-gray-400">
            登入即表示您同意我們的服務條款和隱私政策
          </p>

          <div className="text-xs text-center text-gray-400 mt-4">
            如遇到登入問題，請確認：
            <br />• 允許瀏覽器使用第三方 cookies
            <br />• 網路連線正常
            <br />• 使用最新版瀏覽器
          </div>
        </div>
      </div>
    </div>
  )
}
