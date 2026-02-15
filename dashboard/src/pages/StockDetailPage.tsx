import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, XAxis, YAxis } from 'recharts'
import CandlestickChart from '../components/CandlestickChart'
import ChipsTabs from '../components/ChipsTabs'
import { FundamentalCard } from '../components/FundamentalCard'
import { TechnicalCard } from '../components/TechnicalCard'
import IndicatorCharts from '../components/IndicatorCharts'
import IndicatorToggles from '../components/IndicatorToggles'
import PeriodSelector from '../components/PeriodSelector'
import { AlertDetailCard } from '../components/AlertDetailCard'
import { fetchChartData, fetchStockDetail, fetchStockFundamental, fetchStockHistory, type ChartData } from '../lib/api'
import { getChangeColorClass, getRetailChangeColorClass } from '../lib/colorUtils'
import { IndicatorCalculator } from '../lib/indicators'
import type { FundamentalInfo, StockDetail, StockHistory } from '../types/stock'
import { extractDateOnly, formatDateForAxis } from '../utils/dateFormatters'

export default function StockDetailPage() {
  const { stockId } = useParams<{ stockId: string }>()
  const [searchParams, setSearchParams] = useSearchParams()

  const [detail, setDetail] = useState<StockDetail | null>(null)
  const [history, setHistory] = useState<StockHistory | null>(null)
  const [chartData, setChartData] = useState<ChartData | null>(null)
  const [fundamentalInfo, setFundamentalInfo] = useState<FundamentalInfo | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [fundamentalError, setFundamentalError] = useState<string | null>(null)
  const [syncedTime, setSyncedTime] = useState<string | null>(null)
  const [syncedConcentrationData, setSyncedConcentrationData] = useState<{
    weekLabel: string
    date: string
    largeHolders: number
    superLargeHolders: number
    retail: number
  } | null>(null)

  // 從 URL 參數讀取設定
  const [period, setPeriod] = useState(searchParams.get('period') || '3M')
  const [enabledIndicators, setEnabledIndicators] = useState<string[]>(
    searchParams.get('indicators')?.split(',').filter(Boolean) || ['MA']
  )

  // 檢測深色模式
  const [darkMode, setDarkMode] = useState(
    document.documentElement.classList.contains('dark')
  )

  useEffect(() => {
    // 監聽主題變化
    const observer = new MutationObserver(() => {
      setDarkMode(document.documentElement.classList.contains('dark'))
    })

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    })

    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    if (stockId) {
      loadData()
    }
  }, [stockId, period])  // 移除 enabledIndicators 依賴，指標在前端計算

  // 更新瀏覽器標題
  useEffect(() => {
    // 保存原始標題
    const originalTitle = document.title

    if (detail) {
      // 設定為股票代號和名稱
      document.title = `${detail.basic_info.stock_id} ${detail.basic_info.name} - 台股籌碼集中度儀表板`
    }

    // 組件卸載時恢復原標題
    return () => {
      document.title = originalTitle
    }
  }, [detail])

  // 更新 URL 參數
  useEffect(() => {
    const params: Record<string, string> = {}
    if (period !== '3M') params.period = period
    if (enabledIndicators.length > 0 && enabledIndicators.join(',') !== 'MA') {
      params.indicators = enabledIndicators.join(',')
    }
    setSearchParams(params, { replace: true })
  }, [period, enabledIndicators])

  const loadData = async () => {
    if (!stockId) return

    try {
      setLoading(true)
      setError(null)
      setFundamentalError(null)

      // 並行請求（包含基本面）
      const [detailData, historyData, chartDataResponse, fundData] = await Promise.all([
        fetchStockDetail(stockId),
        fetchStockHistory(stockId, 12),
        fetchChartData(stockId, period),  // 不傳 indicators，由前端計算
        fetchStockFundamental(stockId).catch((err) => {
          setFundamentalError(err instanceof Error ? err.message : '載入基本面資訊失敗')
          return null
        })  // 失敗不影響其他資料
      ])

      setDetail(detailData)
      setHistory(historyData)
      setChartData(chartDataResponse)
      if (fundData) {
        setFundamentalInfo(fundData)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '載入失敗')
    } finally {
      setLoading(false)
    }
  }

  // 前端即時計算技術指標
  const calculatedIndicators = useMemo(() => {
    if (!chartData || !chartData.ohlcv || chartData.ohlcv.length === 0) {
      return {}
    }
    return IndicatorCalculator.calculateAll(chartData.ohlcv, enabledIndicators)
  }, [chartData, enabledIndicators])

  const handlePeriodChange = (newPeriod: string) => {
    setPeriod(newPeriod)
  }

  const handleIndicatorsChange = (newIndicators: string[]) => {
    setEnabledIndicators(newIndicators)
  }

  // 使用 useCallback 確保函數引用穩定，避免圖表頻繁重建
  const handleCrosshairMove = useCallback((time: string | null) => {
    setSyncedTime(time)

    if (time && history?.data) {
      const matched = history.data.find(d =>
        extractDateOnly(d.date) >= extractDateOnly(time)
      )

      if (matched) {
        setSyncedConcentrationData({
          weekLabel: matched.week_label,
          date: extractDateOnly(matched.date),
          largeHolders: matched.moreThan400_pct,
          superLargeHolders: matched.moreThan1000_pct,
          retail: matched.lessThan20_pct,
        })
      } else {
        setSyncedConcentrationData(null)
      }
    } else {
      setSyncedConcentrationData(null)
    }
  }, [history?.data])

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="text-lg text-gray-600 dark:text-gray-400">載入中...</div>
      </div>
    )
  }

  if (error || !detail) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p className="text-red-800 dark:text-red-200">錯誤: {error || '查無此股票'}</p>
        <Link to="/" className="mt-2 inline-block text-primary hover:underline">
          ← 返回列表
        </Link>
      </div>
    )
  }

  return (
    <div>
      {/* 標題與返回 */}
      <div className="mb-6">
        <Link to="/" className="text-primary hover:underline text-sm mb-2 inline-block">
          ← 返回列表
        </Link>
        <div className="flex items-baseline gap-4">
          <h2 className="text-2xl font-heading font-semibold text-gray-900 dark:text-gray-100">
            {detail.basic_info.stock_id} {detail.basic_info.name}
          </h2>
          <div className="text-3xl font-semibold text-gray-900 dark:text-gray-100">
            ${detail.price_info.close.toFixed(2)}
          </div>
          <div className={`text-lg ${detail.price_info.change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {detail.price_info.change >= 0 ? '▲' : '▼'} {detail.price_info.change.toFixed(2)} ({detail.price_info.change_percent.toFixed(2)}%)
          </div>
        </div>
      </div>

      {/* 警示卡片（注意股/處置股） */}
      {detail.alert_status && (
        <AlertDetailCard alertStatus={detail.alert_status} stockId={stockId || ''} />
      )}

      {/* K 線圖區域 */}
      {chartData && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-heading font-semibold text-gray-900 dark:text-gray-100">
              股價走勢
            </h3>
            <PeriodSelector value={period} onChange={handlePeriodChange} />
          </div>

          <CandlestickChart
            data={chartData.ohlcv}
            indicators={calculatedIndicators}
            onCrosshairMove={handleCrosshairMove}
            darkMode={darkMode}
          />

          <div className="mt-4">
            <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">技術指標：</h4>
            <IndicatorToggles
              enabled={enabledIndicators}
              onChange={handleIndicatorsChange}
            />
          </div>
        </div>
      )}

      {/* 籌碼集中度趨勢圖（與K線圖同步） */}
      {history && history.data.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h3 className="text-lg font-heading font-semibold text-gray-900 dark:text-gray-100 mb-4">
            籌碼集中度趨勢（近 12 週）
          </h3>

          {/* 籌碼數值資訊欄（同步顯示） */}
          {syncedConcentrationData && (
            <div className="flex flex-wrap gap-3 mb-3 px-3 py-2 bg-gray-50 dark:bg-gray-800 rounded text-xs border border-gray-200 dark:border-gray-700">
              <span className="font-semibold text-gray-900 dark:text-gray-100">
                {syncedConcentrationData.date}
              </span>
              <span className="text-blue-600 dark:text-blue-400 font-medium">
                大戶 {syncedConcentrationData.largeHolders.toFixed(2)}%
              </span>
              <span className="text-orange-600 dark:text-orange-400 font-medium">
                超大戶 {syncedConcentrationData.superLargeHolders.toFixed(2)}%
              </span>
              <span className="text-red-600 dark:text-red-400 font-medium">
                散戶 {syncedConcentrationData.retail.toFixed(2)}%
              </span>
            </div>
          )}

          <ResponsiveContainer width="100%" height={250}>
            <LineChart
              data={history.data}
              onMouseMove={(e: any) => {
                if (e && e.activeLabel) {
                  const dateOnly = extractDateOnly(e.activeLabel)
                  setSyncedTime(dateOnly)

                  // 找到第一個日期 >= 籌碼圖日期的籌碼數據
                  const matched = history.data.find(d =>
                    extractDateOnly(d.date) >= dateOnly
                  )

                  if (matched) {
                    setSyncedConcentrationData({
                      weekLabel: matched.week_label,
                      date: extractDateOnly(matched.date),
                      largeHolders: matched.moreThan400_pct,
                      superLargeHolders: matched.moreThan1000_pct,
                      retail: matched.lessThan20_pct,
                    })
                  }
                }
              }}
              onMouseLeave={() => {
                setSyncedTime(null)
                setSyncedConcentrationData(null)
              }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#E5E7EB'} />
              <XAxis
                dataKey="date"
                stroke={darkMode ? '#9CA3AF' : '#6B7280'}
                tickFormatter={formatDateForAxis}
              />
              <YAxis stroke={darkMode ? '#9CA3AF' : '#6B7280'} />
              <Legend />
              <Line
                type="monotone"
                dataKey="moreThan400_pct"
                stroke="#3B82F6"
                name="大戶 >400 張"
                strokeWidth={2}
              />
              <Line
                type="monotone"
                dataKey="moreThan1000_pct"
                stroke="#F97316"
                name="超大戶 >1000 張"
                strokeWidth={2}
              />
              <Line
                type="monotone"
                dataKey="lessThan20_pct"
                stroke="#EF4444"
                name="散戶 <20 張"
                strokeWidth={2}
              />
              {/* 同步的 Crosshair 線 */}
              {syncedTime && (() => {
                // 找到第一個日期 >= K線時間的籌碼數據
                const matchedItem = history.data.find(d =>
                  extractDateOnly(d.date) >= extractDateOnly(syncedTime)
                )

                return matchedItem ? (
                  <ReferenceLine
                    x={matchedItem.date}
                    stroke="#9CA3AF"
                    strokeDasharray="3 3"
                    strokeWidth={1}
                  />
                ) : null
              })()}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* 技術指標子圖表 */}
      {chartData && calculatedIndicators && (
        <IndicatorCharts
          data={chartData.ohlcv}
          indicators={calculatedIndicators}
          darkMode={darkMode}
        />
      )}

      {/* 基本資訊卡片 */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">產業</h3>
          <p className="text-lg font-semibold text-gray-900 dark:text-gray-100">
            {detail.basic_info.industry || '未知'}
          </p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">訊號強度</h3>
          <p className="text-lg font-semibold text-gray-900 dark:text-gray-100">
            {detail.signal_strength}
          </p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">籌碼集中度</h3>
          <p className="text-sm text-gray-900 dark:text-gray-100">
            大戶: {detail.concentration_summary.large_holders_pct.toFixed(2)}%
            <br />
            散戶: {detail.concentration_summary.retail_pct.toFixed(2)}%
          </p>
        </div>
      </div>


      {/* 每週變化表格 */}
      {history && history.data.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h3 className="text-lg font-heading font-semibold text-gray-900 dark:text-gray-100">
              每週籌碼變化
            </h3>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">日期</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">大戶持股 (%)</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">大戶變化</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">超大戶持股 (%)</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">超大戶變化</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">散戶持股 (%)</th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">散戶變化</th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                {history.data.slice().reverse().map((point, index) => {
                  // 重新計算週次標籤：最新的為 W1
                  const weekLabel = `W${index + 1}`;

                  return (
                    <tr key={weekLabel}>
                      {/* 日期 */}
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                        {new Date(point.date).toLocaleDateString('zh-TW')}
                      </td>

                      {/* 大戶持股（絕對值） */}
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100 font-medium">
                        {point.moreThan400_pct.toFixed(2)}%
                      </td>

                      {/* 大戶變化 */}
                      <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${getChangeColorClass(point.moreThan400_change)}`}>
                        {point.moreThan400_change >= 0 ? '+' : ''}{point.moreThan400_change.toFixed(2)}%
                      </td>

                      {/* 超大戶持股（絕對值） */}
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100 font-medium">
                        {point.moreThan1000_pct.toFixed(2)}%
                      </td>

                      {/* 超大戶變化 */}
                      <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${getChangeColorClass(point.moreThan1000_change)}`}>
                        {point.moreThan1000_change >= 0 ? '+' : ''}{point.moreThan1000_change.toFixed(2)}%
                      </td>

                      {/* 散戶持股（絕對值） */}
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100 font-medium">
                        {point.lessThan20_pct.toFixed(2)}%
                      </td>

                      {/* 散戶變化 */}
                      <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${getRetailChangeColorClass(point.lessThan20_change)}`}>
                        {point.lessThan20_change >= 0 ? '+' : ''}{point.lessThan20_change.toFixed(2)}%
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* 技術分析卡 */}
      {detail.technical_signals && detail.technical_signals.length > 0 && (
        <TechnicalCard
          technical_signals={detail.technical_signals}
          technical_strength={detail.signal_strength}
          className="mt-6"
        />
      )}

      {/* 基本面資訊卡 */}
      {fundamentalInfo && (
        <FundamentalCard
          info={fundamentalInfo}
          className="mt-6"
        />
      )}

      {/* 基本面載入錯誤 */}
      {fundamentalError && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4 mt-6">
          <p className="text-yellow-800 dark:text-yellow-200 text-sm">
            基本面資訊載入失敗：{fundamentalError}
          </p>
        </div>
      )}

      {/* 籌碼詳細資訊 Tabs */}
      {stockId && (
        <div className="mt-6">
          <ChipsTabs stockId={stockId} />
        </div>
      )}
    </div>
  )
}
