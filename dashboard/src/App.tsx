import { lazy, Suspense } from 'react'
import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'

// Code Splitting: 延遲載入頁面元件
const HomePage = lazy(() => import('./pages/HomePage'))
const StockDetailPage = lazy(() => import('./pages/StockDetailPage'))

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
    <Layout>
      <Suspense fallback={<PageLoading />}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/stocks/:stockId" element={<StockDetailPage />} />
        </Routes>
      </Suspense>
    </Layout>
  )
}

export default App
