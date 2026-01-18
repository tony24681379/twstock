/**
 * 受保護的路由元件
 *
 * 根據 REQUIRE_AUTH 環境變數決定是否需要驗證
 * - REQUIRE_AUTH=false: 允許匿名存取
 * - REQUIRE_AUTH=true: 需要登入才能存取
 */

import React, { ReactNode } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

interface ProtectedRouteProps {
  children: ReactNode
}

export function ProtectedRoute({ children }: ProtectedRouteProps) {
  const { isAuthenticated, isLoading, requireAuth } = useAuth()

  // 如果不需要驗證，直接顯示內容
  if (!requireAuth) {
    return <>{children}</>
  }

  // 等待載入完成
  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <p className="mt-4 text-gray-600 dark:text-gray-400">載入中...</p>
        </div>
      </div>
    )
  }

  // 需要驗證但未登入，重導向到登入頁
  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  // 已登入，顯示內容
  return <>{children}</>
}
