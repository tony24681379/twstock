import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { fetchConvertibleBonds } from '../lib/api'
import type { ConvertibleBondListItem } from '../types/stock'

/** 評分色彩 */
function getScoreBadgeClass(score: number): string {
  if (score >= 80) return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
  if (score >= 60) return 'bg-green-50 text-green-700 dark:bg-green-900/50 dark:text-green-300'
  if (score >= 40) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
  return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
}

/** 溢價率色彩 */
function getPremiumClass(rate: number | null): string {
  if (rate === null) return 'text-gray-400'
  if (rate < 0) return 'text-green-700 dark:text-green-400 font-semibold'
  if (rate < 5) return 'text-green-600 dark:text-green-500'
  if (rate < 15) return 'text-gray-600 dark:text-gray-400'
  return 'text-red-600 dark:text-red-400'
}

/** 風險等級色彩 */
function getRiskBadgeClass(risk: string): string {
  switch (risk) {
    case '強力套利': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    case '套利機會': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
    case '觀望': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
    case '風險警示': return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
    default: return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
  }
}

const SORTABLE_FIELDS = [
  'normalized_score', 'premium_rate', 'arbitrage_spread',
  'volume', 'maturity_date', 'bond_id', 'conversion_value',
] as const

export default function ConvertiblePage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [bonds, setBonds] = useState<ConvertibleBondListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [sortBy, setSortBy] = useState(searchParams.get('sortBy') || 'normalized_score')
  const [order, setOrder] = useState(searchParams.get('order') || 'desc')

  useEffect(() => {
    setSearchParams({ sortBy, order }, { replace: true })
  }, [sortBy, order, setSearchParams])

  useEffect(() => {
    loadBonds()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy, order])

  const loadBonds = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetchConvertibleBonds({ sortBy, order, limit: 500 })
      setBonds(response.data)
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

  const getSortIndicator = (field: string) => {
    if (sortBy !== field) return ''
    return order === 'asc' ? ' ↑' : ' ↓'
  }

  const isSortable = (field: string) => (SORTABLE_FIELDS as readonly string[]).includes(field)

  const thClass = (field: string) => {
    const base = 'px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider'
    if (!isSortable(field)) return base
    return `${base} cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800`
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
        <p className="text-red-800 dark:text-red-200">{error}</p>
        <button onClick={loadBonds} className="mt-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 transition-colors">
          重試
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6">
        <h2 className="text-xl font-heading font-semibold text-gray-900 dark:text-gray-100">
          可轉債套利總表
        </h2>
        <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
          共 {bonds.length} 檔可轉債
        </p>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-900">
              <tr>
                <th onClick={() => handleSort('bond_id')} className={`${thClass('bond_id')} text-left`}>
                  代碼{getSortIndicator('bond_id')}
                </th>
                <th className={`${thClass('')} text-left`}>名稱</th>
                <th className={`${thClass('')} text-left`}>標的股</th>
                <th className={`${thClass('')} text-right`}>CB 收盤價</th>
                <th className={`${thClass('')} text-right`}>轉換價</th>
                <th onClick={() => handleSort('conversion_value')} className={`${thClass('conversion_value')} text-right`}>
                  轉換價值{getSortIndicator('conversion_value')}
                </th>
                <th onClick={() => handleSort('premium_rate')} className={`${thClass('premium_rate')} text-right`}>
                  溢價率{getSortIndicator('premium_rate')}
                </th>
                <th onClick={() => handleSort('arbitrage_spread')} className={`${thClass('arbitrage_spread')} text-right`}>
                  套利空間{getSortIndicator('arbitrage_spread')}
                </th>
                <th onClick={() => handleSort('normalized_score')} className={`${thClass('normalized_score')} text-center`}>
                  套利評分{getSortIndicator('normalized_score')}
                </th>
                <th className={`${thClass('')} text-center`}>風險等級</th>
                <th onClick={() => handleSort('volume')} className={`${thClass('volume')} text-right`}>
                  成交量{getSortIndicator('volume')}
                </th>
                <th onClick={() => handleSort('maturity_date')} className={`${thClass('maturity_date')} text-center`}>
                  到期日{getSortIndicator('maturity_date')}
                </th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
              {bonds.map((bond) => (
                <tr key={bond.bond_id} className="hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors">
                  <td className="px-4 py-3 whitespace-nowrap">
                    <Link
                      to={`/convertible/${bond.bond_id}`}
                      className="text-sm font-medium text-primary hover:underline"
                    >
                      {bond.bond_id}
                    </Link>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                    {bond.name}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm">
                    <Link
                      to={`/stocks/${bond.underlying_stock_id}`}
                      className="text-primary hover:underline"
                    >
                      {bond.underlying_stock_id} {bond.underlying_stock_name}
                    </Link>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                    {bond.close != null ? bond.close.toFixed(2) : '-'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                    {bond.conversion_price != null ? bond.conversion_price.toFixed(2) : '-'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                    {bond.conversion_value != null ? bond.conversion_value.toFixed(2) : '-'}
                  </td>
                  <td className={`px-4 py-3 whitespace-nowrap text-sm text-right ${getPremiumClass(bond.premium_rate)}`}>
                    {bond.premium_rate != null ? `${bond.premium_rate.toFixed(2)}%` : '-'}
                  </td>
                  <td className={`px-4 py-3 whitespace-nowrap text-sm text-right ${
                    bond.arbitrage_spread != null && bond.arbitrage_spread > 0
                      ? 'text-green-700 dark:text-green-400 font-semibold'
                      : 'text-gray-600 dark:text-gray-400'
                  }`}>
                    {bond.arbitrage_spread != null ? `${bond.arbitrage_spread.toFixed(2)}%` : '-'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-center">
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold ${getScoreBadgeClass(bond.normalized_score)}`}>
                      {bond.normalized_score}
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-center">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${getRiskBadgeClass(bond.risk_level)}`}>
                      {bond.risk_level}
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                    {bond.volume != null ? Math.round(bond.volume).toLocaleString() : '-'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-center text-gray-600 dark:text-gray-400">
                    {bond.maturity_date ? new Date(bond.maturity_date).toLocaleDateString('zh-TW') : '-'}
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
