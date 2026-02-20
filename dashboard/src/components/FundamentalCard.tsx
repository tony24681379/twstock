import { useState } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ComposedChart, Bar } from 'recharts'
import type { FundamentalInfo } from '../types/stock'

interface Props {
  info: FundamentalInfo
  className?: string
}

type TabType = 'earning' | 'revenue' | 'valuation' | 'holding' | 'capital' | 'signals'

export function FundamentalCard({ info, className = '' }: Props) {
  const [activeTab, setActiveTab] = useState<TabType>('earning')

  // Tab 配置
  const tabs = [
    { id: 'earning' as TabType, label: '獲利能力', icon: '📊' },
    { id: 'revenue' as TabType, label: '營收成長', icon: '📈' },
    { id: 'valuation' as TabType, label: '估值指標', icon: '💰' },
    { id: 'holding' as TabType, label: '持股結構', icon: '👥' },
    { id: 'capital' as TabType, label: '股本結構', icon: '🏦' },
    {
      id: 'signals' as TabType,
      label: '訊號列表',
      icon: '🔔',
      badge: info.fundamental_signals.length
    },
  ]

  // 強度徽章顏色
  const getStrengthBadgeClass = (strength: number) => {
    if (strength >= 70) return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    if (strength >= 50) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
    if (strength >= 30) return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
    return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
  }

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow ${className}`}>
      {/* 標題與強度分數 */}
      <div className="flex items-center justify-between px-6 pt-6 pb-4">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
          基本面分析
        </h2>
        <div className={`px-4 py-2 rounded-full text-sm font-bold ${getStrengthBadgeClass(info.fundamental_strength)}`}>
          {info.fundamental_strength}
        </div>
      </div>

      {/* Tab 導航列 */}
      <div className="border-b border-gray-200 dark:border-gray-700">
        <nav className="flex -mb-px overflow-x-auto px-6" aria-label="Tabs">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`
                whitespace-nowrap py-3 px-4 border-b-2 font-medium text-sm
                transition-colors duration-200
                ${activeTab === tab.id
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300'
                }
              `}
            >
              <span className="mr-1">{tab.icon}</span>
              {tab.label}
              {tab.badge !== undefined && tab.badge > 0 && (
                <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
                  {tab.badge}
                </span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab 內容區域 */}
      <div className="p-6">
        {activeTab === 'earning' && <EarningTab info={info} />}
        {activeTab === 'revenue' && <RevenueTab info={info} />}
        {activeTab === 'valuation' && <ValuationTab info={info} />}
        {activeTab === 'holding' && <HoldingTab info={info} />}
        {activeTab === 'capital' && <CapitalTab info={info} />}
        {activeTab === 'signals' && <SignalsTab info={info} />}
      </div>
    </div>
  )
}

// ============================================================
// Tab 1: 獲利能力 - EPS 圖表 + 統計 + 詳細表格
// ============================================================
function EarningTab({ info }: { info: FundamentalInfo }) {
  // 準備圖表資料（反轉：最舊→最新）
  const epsChartData = info.eps_recent_4q.map((eps, i) => ({
    quarter: `Q${info.eps_recent_4q.length - i}`,
    eps: eps,
    predicted: null as number | null,
  })).reverse()

  // 加入 EPS 預測虛線點
  const prediction = info.eps_prediction
  if (prediction) {
    // 最後一個實際值也要有 predicted 值（連接虛線起點）
    if (epsChartData.length > 0) {
      epsChartData[epsChartData.length - 1].predicted = epsChartData[epsChartData.length - 1].eps
    }
    epsChartData.push({
      quarter: `Q${prediction.target_quarter}(預測)`,
      eps: null as unknown as number,
      predicted: prediction.value,
    })
  }

  return (
    <div className="space-y-6">
      {/* EPS 趨勢圖 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          最近 4 季 EPS 趨勢
          {prediction && (
            <span className="ml-2 text-xs font-normal text-amber-500">
              含預測值
            </span>
          )}
        </h3>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={epsChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#4B5563" />
              <XAxis
                dataKey="quarter"
                tick={{ fill: '#9CA3AF', fontSize: 12 }}
              />
              <YAxis
                tick={{ fill: '#9CA3AF', fontSize: 12 }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: 'none',
                  borderRadius: '0.5rem',
                  color: '#F3F4F6'
                }}
              />
              <Line
                type="monotone"
                dataKey="eps"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={{ fill: '#3B82F6', r: 4 }}
                connectNulls={false}
              />
              {prediction && (
                <Line
                  type="monotone"
                  dataKey="predicted"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  strokeDasharray="6 3"
                  dot={{ fill: '#f59e0b', r: 4, strokeDasharray: '' }}
                  connectNulls={false}
                  name="預測 EPS"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* EPS 統計指標 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          統計指標
        </h3>
        <div className="grid grid-cols-3 gap-3">
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">趨勢</div>
            <div className="text-base font-semibold text-gray-900 dark:text-white">
              {info.eps_trend}
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">平均 EPS</div>
            <div className="text-base font-semibold text-gray-900 dark:text-white">
              {info.eps_avg.toFixed(2)}
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">標準差</div>
            <div className="text-base font-semibold text-gray-900 dark:text-white">
              {info.eps_stability.toFixed(2)}
            </div>
          </div>
        </div>
      </div>

      {/* 🆕 EPS 詳細表格 */}
      {info.eps_details && info.eps_details.length > 0 && (
        <div>
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
            逐季 EPS 明細
          </h3>
          <div className="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-900">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    年份
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    季度
                  </th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    EPS
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                {info.eps_details.map((detail, idx) => (
                  <tr key={idx}>
                    <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-100">
                      {detail.year}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-100">
                      Q{detail.quarter}
                    </td>
                    <td className="px-4 py-2 text-sm text-right font-mono text-gray-900 dark:text-gray-100">
                      {detail.eps.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ============================================================
// Tab 2: 營收成長 - 月營收趨勢圖 + 統計 + 詳細表格
// ============================================================
function RevenueTab({ info }: { info: FundamentalInfo }) {
  // 檢查是否有營收資料
  if (!info.revenue_details || info.revenue_details.length === 0) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500 dark:text-gray-400 text-sm">
          暫無月營收資料
        </div>
      </div>
    )
  }

  // 準備圖表資料（反轉：最舊→最新）
  const revenueChartData = info.revenue_details.map(detail => ({
    label: `${detail.year}/${detail.month.toString().padStart(2, '0')}`,
    revenue: (detail.revenue / 1000000).toFixed(0), // 轉換為百萬
    yoy_change: detail.yoy_change,
  })).reverse()

  // 計算顏色類別
  const getChangeColorClass = (value: number | null) => {
    if (value === null) return 'text-gray-900 dark:text-gray-100'
    if (value > 0) return 'text-green-600 dark:text-green-400'
    if (value < 0) return 'text-red-600 dark:text-red-400'
    return 'text-gray-900 dark:text-gray-100'
  }

  // 最新月營收（百萬）
  const latestRevenue = info.revenue_details[0]
    ? (info.revenue_details[0].revenue / 1000000).toFixed(0)
    : 'N/A'

  return (
    <div className="space-y-6">
      {/* 月營收趨勢圖 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          最近 12 個月營收趨勢
        </h3>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <ResponsiveContainer width="100%" height={200}>
            <ComposedChart data={revenueChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#4B5563" />
              <XAxis
                dataKey="label"
                tick={{ fill: '#9CA3AF', fontSize: 11 }}
                angle={-45}
                textAnchor="end"
                height={70}
              />
              <YAxis
                yAxisId="left"
                tick={{ fill: '#9CA3AF', fontSize: 12 }}
                label={{ value: '營收（百萬）', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fill: '#9CA3AF', fontSize: 12 }}
                label={{ value: '年增率（%）', angle: 90, position: 'insideRight', fill: '#9CA3AF' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: 'none',
                  borderRadius: '0.5rem',
                  color: '#F3F4F6'
                }}
              />
              <Bar yAxisId="left" dataKey="revenue" fill="#3B82F6" name="月營收（百萬）" />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="yoy_change"
                stroke="#10B981"
                strokeWidth={2}
                dot={{ fill: '#10B981', r: 3 }}
                name="年增率（%）"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* 統計指標 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          統計指標
        </h3>
        <div className="grid grid-cols-3 gap-3">
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">趨勢</div>
            <div className="text-base font-semibold text-gray-900 dark:text-white">
              {info.revenue_trend || '持平'}
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">平均年增率</div>
            <div className={`text-base font-semibold ${getChangeColorClass(info.revenue_yoy_avg || 0)}`}>
              {info.revenue_yoy_avg ? `${info.revenue_yoy_avg.toFixed(2)}%` : 'N/A'}
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">最新月營收</div>
            <div className="text-base font-semibold text-gray-900 dark:text-white">
              {latestRevenue} 百萬
            </div>
          </div>
        </div>
      </div>

      {/* 月營收詳細表格 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          月營收明細
        </h3>
        <div className="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
          <div className="overflow-y-auto max-h-96">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-900 sticky top-0">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    年月
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    月營收<br />(百萬)
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    月增率<br />(%)
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    年增率<br />(%)
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    累計營收<br />(百萬)
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                    累計年增率<br />(%)
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                {info.revenue_details.map((detail, idx) => (
                  <tr key={idx}>
                    <td className="px-3 py-2 text-sm text-gray-900 dark:text-gray-100 whitespace-nowrap">
                      {detail.year}/{detail.month.toString().padStart(2, '0')}
                    </td>
                    <td className="px-3 py-2 text-sm text-right font-mono text-gray-900 dark:text-gray-100">
                      {(detail.revenue / 1000000).toFixed(0)}
                    </td>
                    <td className={`px-3 py-2 text-sm text-right font-mono ${getChangeColorClass(detail.mom_change)}`}>
                      {detail.mom_change !== null
                        ? `${detail.mom_change > 0 ? '+' : ''}${detail.mom_change.toFixed(2)}`
                        : '-'}
                    </td>
                    <td className={`px-3 py-2 text-sm text-right font-mono ${getChangeColorClass(detail.yoy_change)}`}>
                      {detail.yoy_change !== null
                        ? `${detail.yoy_change > 0 ? '+' : ''}${detail.yoy_change.toFixed(2)}`
                        : '-'}
                    </td>
                    <td className="px-3 py-2 text-sm text-right font-mono text-gray-900 dark:text-gray-100">
                      {detail.cumulative_revenue !== null
                        ? (detail.cumulative_revenue / 1000000).toFixed(0)
                        : '-'}
                    </td>
                    <td className={`px-3 py-2 text-sm text-right font-mono ${getChangeColorClass(detail.cumulative_yoy_change)}`}>
                      {detail.cumulative_yoy_change !== null
                        ? `${detail.cumulative_yoy_change > 0 ? '+' : ''}${detail.cumulative_yoy_change.toFixed(2)}`
                        : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============================================================
// Tab 3: 估值指標 - 4 個指標卡片
// ============================================================
function ValuationTab({ info }: { info: FundamentalInfo }) {
  // PSR 文字描述
  const getPsrLabel = (psr: number | null | undefined) => {
    if (psr == null) return { text: '-', color: 'text-gray-400' }
    if (psr <= 1) return { text: '偏低', color: 'text-green-600 dark:text-green-400' }
    if (psr > 10) return { text: '偏高', color: 'text-red-600 dark:text-red-400' }
    return { text: '合理', color: 'text-gray-900 dark:text-white' }
  }
  const psrLabel = getPsrLabel(info.psr)

  // 歷史股利圖表資料（年度由舊到新）
  const dividendChartData = (info.dividend_history ?? [])
    .slice()
    .reverse()
    .map(d => ({
      year: `${d.year}年`,
      cash: d.cash_dividend,
      stock: d.stock_dividend,
    }))

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
        估值指標
      </h3>
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
            本益比 (PER)
          </div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white">
            {info.per !== null ? info.per.toFixed(1) : 'N/A'}
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
            PSR（股價營收比）
          </div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white">
            {info.psr != null ? info.psr.toFixed(2) : '-'}
          </div>
          <div className={`text-xs mt-1 ${psrLabel.color}`}>
            {psrLabel.text}
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
            股利殖利率
          </div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white">
            {info.dividend_yield.toFixed(2)}%
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
            配息率
          </div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white">
            {info.payout_ratio.toFixed(0)}%
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
            現金股利
          </div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white">
            ${info.cash_dividend.toFixed(2)}
          </div>
        </div>
      </div>

      {/* 歷史股利趨勢圖 */}
      {dividendChartData.length > 1 && (
        <div>
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
            歷史股利趨勢
          </h3>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={dividendChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis dataKey="year" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#1f2937',
                    border: 'none',
                    borderRadius: '8px',
                    color: '#f9fafb',
                  }}
                />
                <Bar dataKey="cash" name="現金股利" fill="#10b981" radius={[4, 4, 0, 0]} />
                {dividendChartData.some(d => d.stock > 0) && (
                  <Bar dataKey="stock" name="股票股利" fill="#6366f1" radius={[4, 4, 0, 0]} />
                )}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}

// ============================================================
// Tab 3: 持股結構 - 董監持股 + 三大法人
// ============================================================
function HoldingTab({ info }: { info: FundamentalInfo }) {
  const { holding_info } = info

  if (!holding_info) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500 dark:text-gray-400 text-sm">
          暫無持股結構資料
        </div>
      </div>
    )
  }

  // 格式化日期
  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return 'N/A'
    return new Date(dateStr).toLocaleDateString('zh-TW')
  }

  return (
    <div className="space-y-6">
      {/* 資料日期 */}
      <div className="text-xs text-gray-500 dark:text-gray-400">
        資料日期：{formatDate(holding_info.latest_date)}
      </div>

      {/* 董監持股 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          董監持股
        </h3>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-600 dark:text-gray-400">
              董監持股比率
            </div>
            <div className="text-2xl font-bold text-gray-900 dark:text-white">
              {holding_info.director_ratio !== null
                ? `${holding_info.director_ratio.toFixed(2)}%`
                : 'N/A'}
            </div>
          </div>
        </div>
      </div>

      {/* 三大法人持股 */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
          三大法人持股
        </h3>
        <div className="space-y-3">
          {/* 外資 */}
          <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4 border border-blue-200 dark:border-blue-800">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-blue-700 dark:text-blue-300">
                外資
              </div>
              <div className="text-xl font-bold text-blue-900 dark:text-blue-100">
                {holding_info.foreign_holding_rate !== null
                  ? `${holding_info.foreign_holding_rate.toFixed(2)}%`
                  : 'N/A'}
              </div>
            </div>
          </div>

          {/* 投信 */}
          <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 border border-green-200 dark:border-green-800">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-green-700 dark:text-green-300">
                投信
              </div>
              <div className="text-xl font-bold text-green-900 dark:text-green-100">
                {holding_info.investment_trust_holding_rate !== null
                  ? `${holding_info.investment_trust_holding_rate.toFixed(2)}%`
                  : 'N/A'}
              </div>
            </div>
          </div>

          {/* 自營商 */}
          <div className="bg-orange-50 dark:bg-orange-900/20 rounded-lg p-4 border border-orange-200 dark:border-orange-800">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-orange-700 dark:text-orange-300">
                自營商
              </div>
              <div className="text-xl font-bold text-orange-900 dark:text-orange-100">
                {holding_info.dealer_holding_rate !== null
                  ? `${holding_info.dealer_holding_rate.toFixed(2)}%`
                  : 'N/A'}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============================================================
// Tab 4: 股本結構 - 資本額、股本、股票股利
// ============================================================
function CapitalTab({ info }: { info: FundamentalInfo }) {
  const { capital_info } = info

  if (!capital_info) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500 dark:text-gray-400 text-sm">
          暫無股本結構資料
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
        股本結構
      </h3>
      <div className="space-y-4">
        {/* 實收資本額 */}
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-600 dark:text-gray-400">
              實收資本額
            </div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">
              {capital_info.capital !== null
                ? `${capital_info.capital.toFixed(2)} 百萬`
                : 'N/A'}
            </div>
          </div>
        </div>

        {/* 股本（千股） */}
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-600 dark:text-gray-400">
              股本（千股）
            </div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">
              {capital_info.outstanding_shares !== null
                ? `${capital_info.outstanding_shares.toFixed(0)} 千股`
                : 'N/A'}
            </div>
          </div>
        </div>

        {/* 股票股利 */}
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-600 dark:text-gray-400">
              股票股利
            </div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">
              {capital_info.stock_dividend !== null
                ? `${capital_info.stock_dividend.toFixed(2)}`
                : 'N/A'}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============================================================
// Tab 5: 訊號列表 - 基本面訊號（保持原有實作）
// ============================================================
function SignalsTab({ info }: { info: FundamentalInfo }) {
  // 訊號顏色類別
  const getSignalColorClass = (score: number) => {
    if (score > 0) {
      return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    }
    return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
  }

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
        觸發訊號 ({info.fundamental_signals.length})
      </h3>

      {info.fundamental_signals.length === 0 ? (
        <div className="text-center py-8">
          <div className="text-sm text-gray-500 dark:text-gray-400">
            無觸發訊號
          </div>
        </div>
      ) : (
        <div className="space-y-2">
          {info.fundamental_signals.map((signal, index) => (
            <div
              key={index}
              className={`rounded-lg p-3 ${getSignalColorClass(signal.score)}`}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="font-semibold text-sm">
                  {signal.name}
                </span>
                <span className="font-bold text-sm">
                  {signal.score > 0 ? '+' : ''}{signal.score}
                </span>
              </div>
              {signal.description && (
                <div className="text-xs opacity-80">
                  {signal.description}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
