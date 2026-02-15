/**
 * 身份驗證 Context
 *
 * 使用 Google OAuth 2.0 進行身份驗證
 * 支援環境變數開關驗證功能
 */

import { createContext, useContext, useState, useEffect, ReactNode } from 'react'

interface AuthUser {
  email: string
  name?: string
  picture?: string
}

interface AuthContextType {
  user: AuthUser | null
  token: string | null
  isAuthenticated: boolean
  isLoading: boolean
  requireAuth: boolean
  login: (credential: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return context
}

interface AuthProviderProps {
  children: ReactNode
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // 從環境變數讀取是否需要驗證
  const requireAuth = import.meta.env.VITE_REQUIRE_AUTH === 'true'

  // 從 localStorage 載入已儲存的 token
  useEffect(() => {
    const loadStoredAuth = () => {
      try {
        const storedToken = localStorage.getItem('auth_token')
        const storedUser = localStorage.getItem('auth_user')

        if (storedToken && storedUser) {
          setToken(storedToken)
          setUser(JSON.parse(storedUser))
        }
      } catch (error) {
        console.error('Failed to load stored auth:', error)
        localStorage.removeItem('auth_token')
        localStorage.removeItem('auth_user')
      } finally {
        setIsLoading(false)
      }
    }

    loadStoredAuth()
  }, [])

  const login = async (credential: string) => {
    try {
      // 驗證 credential 格式
      if (!credential || typeof credential !== 'string') {
        throw new Error('Invalid credential format')
      }

      // 解碼 JWT token 取得使用者資訊
      const parts = credential.split('.')
      if (parts.length !== 3) {
        throw new Error('Invalid JWT token format')
      }

      // URL-safe base64 解碼函數
      const base64UrlDecode = (str: string): string => {
        try {
          // 將 URL-safe base64 轉換為標準 base64
          // 替換 - 為 +，_ 為 /
          let base64 = str.replace(/-/g, '+').replace(/_/g, '/')

          // 補齊 padding（=）
          const padding = base64.length % 4
          if (padding > 0) {
            base64 += '='.repeat(4 - padding)
          }

          return atob(base64)
        } catch (e) {
          console.error('Base64 decode error:', e)
          throw new Error('無法解碼 token，請稍後再試')
        }
      }

      const payloadJson = base64UrlDecode(parts[1])
      const payload = JSON.parse(payloadJson)

      // 驗證必要欄位
      if (!payload.email) {
        throw new Error('Token 中缺少 email 資訊')
      }

      const authUser: AuthUser = {
        email: payload.email,
        name: payload.name,
        picture: payload.picture,
      }

      console.log('Login successful for:', authUser.email)

      // 儲存到 state 和 localStorage
      setToken(credential)
      setUser(authUser)
      localStorage.setItem('auth_token', credential)
      localStorage.setItem('auth_user', JSON.stringify(authUser))
    } catch (error) {
      console.error('Login failed:', error)
      // 提供更詳細的錯誤訊息
      if (error instanceof Error) {
        throw new Error(`認證失敗: ${error.message}`)
      }
      throw new Error('認證處理失敗，請稍後再試')
    }
  }

  const logout = () => {
    setToken(null)
    setUser(null)
    localStorage.removeItem('auth_token')
    localStorage.removeItem('auth_user')
  }

  const value: AuthContextType = {
    user,
    token,
    isAuthenticated: !!user,
    isLoading,
    requireAuth,
    login,
    logout,
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}
