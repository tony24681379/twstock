/**
 * 身份驗證 Context
 *
 * 使用 Google OAuth 2.0 進行身份驗證
 * 支援環境變數開關驗證功能
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'

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
      // 解碼 JWT token 取得使用者資訊
      const payload = JSON.parse(atob(credential.split('.')[1]))

      const authUser: AuthUser = {
        email: payload.email,
        name: payload.name,
        picture: payload.picture,
      }

      // 儲存到 state 和 localStorage
      setToken(credential)
      setUser(authUser)
      localStorage.setItem('auth_token', credential)
      localStorage.setItem('auth_user', JSON.stringify(authUser))
    } catch (error) {
      console.error('Login failed:', error)
      throw new Error('Failed to process authentication')
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
