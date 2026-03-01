import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { PAGINATION } from '../constants/apiConfig'
import { fetchStocks } from '../lib/api'
import { getSignalStrengthBadgeClass, getRawScoreBadgeClass } from '../lib/colorUtils'
import { WeightController } from '../components/WeightController'
import { SignalFilter } from '../components/SignalFilter'
import SignalTooltip from '../components/SignalTooltip'
import { AlertIcon } from '../components/AlertIcon'
import type { StockListItem } from '../types/stock'

export default function HomePage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [stocks, setStocks] = useState<StockListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeSignals, setActiveSignals] = useState<string[]>([])

  // 從 URL 讀取排序參數，預設為綜合強度降序
  const [sortBy, setSortBy] = useState(searchParams.get('sortBy') || 'overall_strength')
  const [order, setOrder] = useState(searchParams.get('order') || 'desc')

  // 權重狀態（從 localStorage 讀取，與 WeightController 保持一致）
  const [weights, setWeights] = useState(() => {
    const saved = localStorage.getItem('strengthWeights')
    if (saved) {
      try {
        const parsed = JSON.parse(saved)
        // 檢查是否為三維權重格式，且總和為 1.0
        if (parsed.chip !== undefined && parsed.technical !== undefined && parsed.fundamental !== undefined) {
          const sum = parsed.chip + parsed.technical + parsed.fundamental
          if (Math.abs(sum - 1.0) < 0.01) {
            return parsed
          }
        }
      } catch {
        // fallback to default
      }
    }
    // 使用預設值並儲存到 localStorage
    const defaultWeights = { chip: 0.4, technical: 0.3, fundamental: 0.3 }
    localStorage.setItem('strengthWeights', JSON.stringify(defaultWeights))
    return defaultWeights
  })

  // 同步排序狀態到 URL
  useEffect(() => {
    setSearchParams({
      sortBy,
      order
    }, { replace: true })
  }, [sortBy, order, setSearchParams])

  // 載入股票資料（依賴排序參數和權重的各個屬性）
  useEffect(() => {
    loadStocks()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy, order, weights.chip, weights.technical, weights.fundamental])

  const loadStocks = async () => {
    try {
      setLoading(true)
      setError(null)
      // 載入所有股票，傳遞權重參數（籌碼 + 技術 + 基本面）
      const response = await fetchStocks({
        sortBy,
        order,
        limit: PAGINATION.LOAD_ALL_LIMIT,
        offset: 0,
        chipWeight: weights.chip,
        techWeight: weights.technical,
        fundWeight: weights.fundamental
      })
      setStocks(response.data)
    } catch (err) {
      setError(err instanceof Error ? err.message : '載入失敗')
    } finally {
      setLoading(false)
    }
  }

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setOrder(order === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(field)
      setOrder('desc')
    }
  }

  // 訊號篩選（AND 邏輯）
  const filteredStocks = useMemo(() => {
    if (activeSignals.length === 0) return stocks
    return stocks.filter(stock => {
      const stockSignalNames = new Set([
        ...(stock.chip_signals || []).map(s => s.name),
        ...(stock.technical_signals || []).map(s => s.name),
        ...(stock.fundamental_signals || []).map(s => s.name),
        ...(stock.recent_events || []).map(s => s.name),
        ...(stock.cb_signals || []).map(s => s.name),
      ])
      return activeSignals.every(sig => stockSignalNames.has(sig))
    })
  }, [stocks, activeSignals])

  const getSortIndicator = (field: string) => {
    if (sortBy !== field) return ''
    return order === 'asc' ? ' ↑' : ' ↓'
  }

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="text-lg text-gray-600 dark:text-gray-400">載入中...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p className="text-red-800 dark:text-red-200">錯誤: {error}</p>
        <button
          onClick={loadStocks}
          className="mt-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
        >
          重試
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6">
        <h2 className="text-xl font-heading font-semibold text-gray-900 dark:text-gray-100">
          股票列表
        </h2>
        <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
          {activeSignals.length > 0
            ? `符合條件: ${filteredStocks.length} / ${stocks.length} 支股票`
            : `共 ${stocks.length} 支股票（已載入全部）`}
        </p>
      </div>

      {/* 權重控制器 */}
      <WeightController
        onChange={setWeights}
        className="mb-6"
      />

      {/* 訊號篩選 */}
      <SignalFilter
        activeSignals={activeSignals}
        onSignalsChange={setActiveSignals}
        stocks={stocks}
        className="mb-6"
      />

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-900">
              <tr>
                <th
                  onClick={() => handleSort('stock_id')}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  股票代碼{getSortIndicator('stock_id')}
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  股票名稱
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  收盤價
                </th>
                {/* 三種強度欄位（籌碼 + 技術 + 基本面）*/}
                <th
                  onClick={() => handleSort('chip_strength')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  籌碼強度{getSortIndicator('chip_strength')}
                </th>
                <th
                  onClick={() => handleSort('technical_strength')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  技術強度{getSortIndicator('technical_strength')}
                </th>
                <th
                  onClick={() => handleSort('fundamental_strength')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  基本面強度{getSortIndicator('fundamental_strength')}
                </th>
                <th
                  onClick={() => handleSort('overall_strength')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  綜合強度{getSortIndicator('overall_strength')}
                </th>
                <th
                  onClick={() => handleSort('cb_arbitrage_score')}
                  className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800"
                >
                  CB 套利{getSortIndicator('cb_arbitrage_score')}
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  訊號數量
                </th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
              {filteredStocks.map((stock) => (
                <tr
                  key={stock.stock_id}
                  className="hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer transition-colors"
                >
                  <td className="px-6 py-4 whitespace-nowrap">
                    <Link
                      to={`/stocks/${stock.stock_id}`}
                      className="text-sm font-medium text-primary hover:underline inline-flex items-center"
                    >
                      {stock.stock_id}
                      <AlertIcon alertStatus={stock.alert_status} />
                    </Link>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                    {stock.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                    ${stock.close_price.toFixed(2)}
                  </td>

                  {/* 籌碼強度 */}
                  <td className="px-6 py-4 whitespace-nowrap text-center relative group">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getRawScoreBadgeClass(stock.chip_strength)}`}>
                      {stock.chip_strength > 0 ? '+' : ''}{stock.chip_strength}
                    </span>

                    {/* 籌碼訊號 Tooltip */}
                    {stock.chip_signals && stock.chip_signals.length > 0 && (
                      <SignalTooltip
                        title={`籌碼訊號 (${stock.chip_signals.length})`}
                        signals={stock.chip_signals}
                        show={true}
                      />
                    )}
                  </td>

                  {/* 技術強度 */}
                  <td className="px-6 py-4 whitespace-nowrap text-center relative group">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getRawScoreBadgeClass(stock.technical_strength)}`}>
                      {stock.technical_strength > 0 ? '+' : ''}{stock.technical_strength}
                    </span>

                    {/* 技術訊號 Tooltip */}
                    {stock.technical_signals && stock.technical_signals.length > 0 && (
                      <SignalTooltip
                        title={`技術訊號 (${stock.technical_signals.length})`}
                        signals={stock.technical_signals}
                        show={true}
                      />
                    )}
                  </td>

                  {/* 基本面強度 */}
                  <td className="px-6 py-4 whitespace-nowrap text-center relative group">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getRawScoreBadgeClass(stock.fundamental_strength)}`}>
                      {stock.fundamental_strength > 0 ? '+' : ''}{stock.fundamental_strength}
                    </span>

                    {/* 基本面訊號 Tooltip */}
                    {stock.fundamental_signals && stock.fundamental_signals.length > 0 && (
                      <SignalTooltip
                        title={`基本面訊號 (${stock.fundamental_signals.length})`}
                        signals={stock.fundamental_signals}
                        show={true}
                      />
                    )}
                  </td>

                  {/* 綜合強度（加粗 + Tooltip）*/}
                  <td className="px-6 py-4 whitespace-nowrap text-center relative group">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold ${getSignalStrengthBadgeClass(stock.overall_strength)}`}>
                      {stock.overall_strength}
                    </span>

                    {/* Tooltip：顯示三種訊號（籌碼 + 技術 + 基本面）*/}
                    {((stock.chip_signals && stock.chip_signals.length > 0) ||
                      (stock.technical_signals && stock.technical_signals.length > 0) ||
                      (stock.fundamental_signals && stock.fundamental_signals.length > 0)) && (
                      <div className="hidden group-hover:block absolute top-full left-1/2 -translate-x-1/2 mt-2
                                      bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900
                                      rounded-lg px-3 py-2 text-xs shadow-xl z-50 min-w-[300px] max-h-[500px] overflow-y-auto
                                      before:content-[''] before:absolute before:bottom-full before:left-1/2 before:-translate-x-1/2
                                      before:border-4 before:border-transparent before:border-b-gray-900 dark:before:border-b-gray-100">

                        {/* 籌碼訊號 */}
                        {stock.chip_signals && stock.chip_signals.length > 0 && (
                          <div>
                            <div className="font-semibold mb-1.5 border-b border-gray-700 dark:border-gray-300 pb-1">
                              籌碼訊號 ({stock.chip_signals.length}):
                            </div>
                            <div className="space-y-0.5">
                              {stock.chip_signals.map((signal, idx) => (
                                <div key={idx} className="flex justify-between gap-3">
                                  <span className="text-left">• {signal.name}</span>
                                  <span className={`font-semibold ${
                                    signal.score > 0 ? 'text-green-400 dark:text-green-600' : 'text-red-400 dark:text-red-600'
                                  }`}>
                                    {signal.score > 0 ? '+' : ''}{signal.score}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* 技術訊號 */}
                        {stock.technical_signals && stock.technical_signals.length > 0 && (
                          <div className="border-t border-gray-700 dark:border-gray-300 mt-2 pt-2">
                            <div className="font-semibold mb-1.5 border-b border-gray-700 dark:border-gray-300 pb-1">
                              技術訊號 ({stock.technical_signals.length}):
                            </div>
                            <div className="space-y-0.5">
                              {stock.technical_signals.map((signal, idx) => (
                                <div key={idx} className="flex justify-between gap-3">
                                  <span className="text-left">• {signal.name}</span>
                                  <span className={`font-semibold ${
                                    signal.score > 0 ? 'text-green-400 dark:text-green-600' : 'text-red-400 dark:text-red-600'
                                  }`}>
                                    {signal.score > 0 ? '+' : ''}{signal.score}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* 基本面訊號 */}
                        {stock.fundamental_signals && stock.fundamental_signals.length > 0 && (
                          <div className="border-t border-gray-700 dark:border-gray-300 mt-2 pt-2">
                            <div className="font-semibold mb-1.5 border-b border-gray-700 dark:border-gray-300 pb-1">
                              基本面訊號 ({stock.fundamental_signals.length}):
                            </div>
                            <div className="space-y-0.5">
                              {stock.fundamental_signals.map((signal, idx) => (
                                <div key={idx} className="flex justify-between gap-3">
                                  <span className="text-left">• {signal.name}</span>
                                  <span className={`font-semibold ${
                                    signal.score > 0 ? 'text-green-400 dark:text-green-600' : 'text-red-400 dark:text-red-600'
                                  }`}>
                                    {signal.score > 0 ? '+' : ''}{signal.score}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </td>

                  {/* CB 套利分數 */}
                  <td className="px-6 py-4 whitespace-nowrap text-center relative group">
                    {stock.cb_arbitrage_score != null ? (
                      <>
                        <Link
                          to={`/convertible?sortBy=normalized_score&order=desc`}
                          onClick={(e) => e.stopPropagation()}
                          className="hover:opacity-80"
                        >
                          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold ${getSignalStrengthBadgeClass(stock.cb_arbitrage_score)}`}>
                            {stock.cb_arbitrage_score}
                          </span>
                        </Link>
                        {stock.cb_signals && stock.cb_signals.length > 0 && (
                          <SignalTooltip
                            title={`CB 套利訊號 (${stock.cb_signals.length})`}
                            signals={stock.cb_signals}
                            show={true}
                          />
                        )}
                      </>
                    ) : (
                      <span className="text-xs text-gray-400 dark:text-gray-600">-</span>
                    )}
                  </td>

                  <td className="px-6 py-4 whitespace-nowrap text-sm text-center text-gray-900 dark:text-gray-100">
                    {stock.signal_count}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
