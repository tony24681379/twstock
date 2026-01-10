import { memo, useEffect, useState } from 'react'
import { fetchChipsData, type ChipsData, type InstitutionalData, type MajorInvestorData, type MarginTradingData } from '../lib/api'

interface ChipsTabsProps {
  stockId: string
}

type TabType = 'institutional' | 'major' | 'margin' | 'concentration'

function ChipsTabs({ stockId }: ChipsTabsProps) {
  const [activeTab, setActiveTab] = useState<TabType>('institutional')
  const [chipsData, setChipsData] = useState<ChipsData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true)
        setError(null)
        const data = await fetchChipsData(stockId, 30)
        setChipsData(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : '載入失敗')
      } finally {
        setLoading(false)
      }
    }

    loadData()
  }, [stockId])

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('zh-TW', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    })
  }

  const formatNumber = (num: number | null) => {
    if (num === null || num === undefined) return 'N/A'
    return num.toFixed(2)
  }

  const formatNumberWithSign = (num: number | null) => {
    if (num === null || num === undefined) return 'N/A'
    const formatted = num.toFixed(2)
    if (num > 0) {
      return <span className="text-red-600 dark:text-red-400">+{formatted}</span>
    } else if (num < 0) {
      return <span className="text-green-600 dark:text-green-400">{formatted}</span>
    }
    return formatted
  }

  if (loading) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-10 bg-gray-200 dark:bg-gray-700 rounded mb-4"></div>
          <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
        </div>
      </div>
    )
  }

  if (error || !chipsData) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <p className="text-red-600 dark:text-red-400">
          {error || '無籌碼資料'}
        </p>
      </div>
    )
  }

  const tabs = [
    { id: 'institutional' as TabType, label: '三大法人', count: chipsData.institutional.length },
    { id: 'major' as TabType, label: '主力買賣', count: chipsData.major.length },
    { id: 'margin' as TabType, label: '融資融券', count: chipsData.margin.length },
    { id: 'concentration' as TabType, label: '籌碼集中度', count: chipsData.concentration ? 1 : 0 },
  ]

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
      {/* Tab Headers */}
      <div className="border-b border-gray-200 dark:border-gray-700">
        <nav className="flex -mb-px">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`
                px-6 py-3 text-sm font-medium border-b-2 transition-colors
                ${activeTab === tab.id
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300'
                }
              `}
            >
              {tab.label}
              {tab.count > 0 && (
                <span className="ml-2 text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded-full">
                  {tab.count}
                </span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="p-6">
        {activeTab === 'institutional' && (
          <InstitutionalTable data={chipsData.institutional} formatDate={formatDate} formatNumberWithSign={formatNumberWithSign} formatNumber={formatNumber} />
        )}
        {activeTab === 'major' && (
          <MajorTable data={chipsData.major} formatDate={formatDate} formatNumberWithSign={formatNumberWithSign} />
        )}
        {activeTab === 'margin' && (
          <MarginTable data={chipsData.margin} formatDate={formatDate} formatNumber={formatNumber} />
        )}
        {activeTab === 'concentration' && (
          <ConcentrationSummary data={chipsData.concentration} formatDate={formatDate} formatNumber={formatNumber} />
        )}
      </div>
    </div>
  )
}

// 三大法人表格
function InstitutionalTable({
  data,
  formatDate,
  formatNumberWithSign,
  formatNumber
}: {
  data: InstitutionalData[]
  formatDate: (date: string) => string
  formatNumberWithSign: (num: number | null) => JSX.Element | string
  formatNumber: (num: number | null) => string
}) {
  if (data.length === 0) {
    return <p className="text-gray-500 dark:text-gray-400">無資料</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-900">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">日期</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">外資 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">投信 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">自營商 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">三大法人持股 (%)</th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          {data.map((row, index) => (
            <tr key={index} className="hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
              <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                {formatDate(row.date)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.foreign)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.investment_trust)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.dealer)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                {formatNumber(row.sum_holding_rate)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// 主力買賣表格
function MajorTable({
  data,
  formatDate,
  formatNumberWithSign
}: {
  data: MajorInvestorData[]
  formatDate: (date: string) => string
  formatNumberWithSign: (num: number | null) => JSX.Element | string
}) {
  if (data.length === 0) {
    return <p className="text-gray-500 dark:text-gray-400">無資料</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-900">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">日期</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">主力買賣超 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">分點差額 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">20日主力 (張)</th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          {data.map((row, index) => (
            <tr key={index} className="hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
              <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                {formatDate(row.date)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.major_investors)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.agent_diff)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                {formatNumberWithSign(row.skp20)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// 融資融券表格
function MarginTable({
  data,
  formatDate,
  formatNumber
}: {
  data: MarginTradingData[]
  formatDate: (date: string) => string
  formatNumber: (num: number | null) => string
}) {
  if (data.length === 0) {
    return <p className="text-gray-500 dark:text-gray-400">無資料</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-900">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">日期</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">融資餘額 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">融券餘額 (張)</th>
            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">額度限制</th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          {data.map((row, index) => (
            <tr key={index} className="hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
              <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                {formatDate(row.date)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                {formatNumber(row.lending_balance)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                {formatNumber(row.borrowing_balance)}
              </td>
              <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                {formatNumber(row.balance_limit)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// 籌碼集中度摘要
function ConcentrationSummary({
  data,
  formatDate,
  formatNumber
}: {
  data: ChipsData['concentration']
  formatDate: (date: string) => string
  formatNumber: (num: number | null) => string
}) {
  if (!data) {
    return <p className="text-gray-500 dark:text-gray-400">無資料</p>
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">大戶持股 (&gt;400張)</h4>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
          {formatNumber(data.large_holders_pct)}%
        </p>
      </div>

      <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">超大戶持股 (&gt;1000張)</h4>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
          {formatNumber(data.super_large_holders_pct)}%
        </p>
      </div>

      <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">散戶持股 (&lt;20張)</h4>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
          {formatNumber(data.retail_pct)}%
        </p>
      </div>

      <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">最新資料日期</h4>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
          {formatDate(data.latest_date)}
        </p>
      </div>
    </div>
  )
}

// 使用 React.memo 避免不必要的重渲染
export default memo(ChipsTabs)
