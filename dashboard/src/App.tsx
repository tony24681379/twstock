import { lazy, Suspense } from 'react'
import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import { ProtectedRoute } from './components/ProtectedRoute'
import { LoginPage } from './pages/LoginPage'

// Code Splitting: 延遲載入頁面元件
const HomePage = lazy(() => import('./pages/HomePage'))
const StockDetailPage = lazy(() => import('./pages/StockDetailPage'))
const ConvertiblePage = lazy(() => import('./pages/ConvertiblePage'))
const ConvertibleDetailPage = lazy(() => import('./pages/ConvertibleDetailPage'))

// Loading 元件
function PageLoading() {
  return (
    <div className="flex justify-center items-center h-64">
      <div className="text-lg text-gray-600 dark:text-gray-400">載入中...</div>
    </div>
  )
}

function App() {
  return (
    <Routes>
      {/* 登入頁面（不需要驗證） */}
      <Route path="/login" element={<LoginPage />} />

      {/* 受保護的路由（需要驗證） */}
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <Layout>
              <Suspense fallback={<PageLoading />}>
                <Routes>
                  <Route path="/" element={<HomePage />} />
                  <Route path="/stocks/:stockId" element={<StockDetailPage />} />
                  <Route path="/convertible" element={<ConvertiblePage />} />
                  <Route path="/convertible/:bondId" element={<ConvertibleDetailPage />} />
                </Routes>
              </Suspense>
            </Layout>
          </ProtectedRoute>
        }
      />
    </Routes>
  )
}

export default App
