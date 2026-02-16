import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { fetchConvertibleBondDetail, fetchConvertibleBondHistory } from '../lib/api'
import type { ConvertibleBondDetail, ConvertibleBondHistoryPoint } from '../types/stock'

function getScoreBadgeClass(score: number): string {
  if (score >= 80) return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
  if (score >= 60) return 'bg-green-50 text-green-700 dark:bg-green-900/50 dark:text-green-300'
  if (score >= 40) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
  return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
}

function getRiskBadgeClass(risk: string): string {
  switch (risk) {
    case '強力套利': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    case '套利機會': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
    case '觀望': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
    case '風險警示': return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
    default: return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
  }
}

export default function ConvertibleDetailPage() {
  const { bondId } = useParams<{ bondId: string }>()
  const [bond, setBond] = useState<ConvertibleBondDetail | null>(null)
  const [history, setHistory] = useState<ConvertibleBondHistoryPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!bondId) return
    loadData()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bondId])

  const loadData = async () => {
    setLoading(true)
    setError(null)
    try {
      const [detail, hist] = await Promise.all([
        fetchConvertibleBondDetail(bondId!),
        fetchConvertibleBondHistory(bondId!, 90),
      ])
      setBond(detail)
      setHistory(hist)
    } catch (err) {
      setError(err instanceof Error ? err.message : '載入失敗')
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="text-lg text-gray-600 dark:text-gray-400">載入中...</div>
      </div>
    )
  }

  if (error || !bond) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p className="text-red-800 dark:text-red-200">{error || '找不到資料'}</p>
        <Link to="/convertible" className="mt-2 inline-block text-primary hover:underline">
          返回列表
        </Link>
      </div>
    )
  }

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <Link to="/convertible" className="text-sm text-primary hover:underline mb-2 inline-block">
          &larr; 返回可轉債列表
        </Link>
        <h2 className="text-xl font-heading font-semibold text-gray-900 dark:text-gray-100">
          {bond.bond_id} {bond.name}
        </h2>
        <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
          標的股：
          <Link to={`/stocks/${bond.underlying_stock_id}`} className="text-primary hover:underline">
            {bond.underlying_stock_id} {bond.underlying_stock_name}
          </Link>
        </p>
      </div>

      {/* 套利指標卡片 */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">套利評分</div>
          <div className="flex items-center gap-2">
            <span className={`text-2xl font-bold px-2 py-0.5 rounded ${getScoreBadgeClass(bond.normalized_score)}`}>
              {bond.normalized_score}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${getRiskBadgeClass(bond.risk_level)}`}>
              {bond.risk_level}
            </span>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">溢價率</div>
          <div className={`text-2xl font-bold ${
            bond.premium_rate != null && bond.premium_rate < 0
              ? 'text-green-700 dark:text-green-400'
              : 'text-gray-900 dark:text-gray-100'
          }`}>
            {bond.premium_rate != null ? `${bond.premium_rate.toFixed(2)}%` : '-'}
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">套利空間</div>
          <div className={`text-2xl font-bold ${
            bond.arbitrage_spread != null && bond.arbitrage_spread > 0
              ? 'text-green-700 dark:text-green-400'
              : 'text-gray-900 dark:text-gray-100'
          }`}>
            {bond.arbitrage_spread != null ? `${bond.arbitrage_spread.toFixed(2)}%` : '-'}
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">轉換價值</div>
          <div className="text-2xl font-bold text-gray-900 dark:text-gray-100">
            {bond.conversion_value != null ? bond.conversion_value.toFixed(2) : '-'}
          </div>
        </div>
      </div>

      {/* 基本資訊 + 訊號 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        {/* 基本資訊 */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-3 border-b border-gray-200 dark:border-gray-700 pb-2">
            基本資訊
          </h3>
          <div className="space-y-2 text-sm">
            <InfoRow label="CB 收盤價" value={bond.close != null ? bond.close.toFixed(2) : '-'} />
            <InfoRow label="標的股收盤" value={bond.underlying_close != null ? bond.underlying_close.toFixed(2) : '-'} />
            <InfoRow label="轉換價格" value={bond.conversion_price != null ? bond.conversion_price.toFixed(2) : '-'} />
            <InfoRow label="票面利率" value={bond.coupon_rate != null ? `${bond.coupon_rate}%` : '-'} />
            <InfoRow label="發行日" value={bond.issue_date ? new Date(bond.issue_date).toLocaleDateString('zh-TW') : '-'} />
            <InfoRow label="到期日" value={bond.maturity_date ? new Date(bond.maturity_date).toLocaleDateString('zh-TW') : '-'} />
            <InfoRow label="賣回日" value={bond.put_date ? new Date(bond.put_date).toLocaleDateString('zh-TW') : '-'} />
            <InfoRow label="賣回價" value={bond.put_price != null ? bond.put_price.toFixed(2) : '-'} />
            <InfoRow label="成交量" value={bond.volume != null ? Math.round(bond.volume).toLocaleString() : '-'} />
            <InfoRow label="標的股綜合強度" value={String(bond.underlying_overall_strength)} />
          </div>
        </div>

        {/* 訊號列表 */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-3 border-b border-gray-200 dark:border-gray-700 pb-2">
            套利訊號 ({bond.signal_count})
          </h3>
          {bond.signals.length === 0 ? (
            <p className="text-sm text-gray-500 dark:text-gray-400">無觸發訊號</p>
          ) : (
            <div className="space-y-2">
              {bond.signals.map((sig, idx) => (
                <div key={idx} className="flex justify-between items-center text-sm">
                  <div>
                    <span className="text-gray-900 dark:text-gray-100">{sig.name}</span>
                    {sig.description && (
                      <span className="text-xs text-gray-500 dark:text-gray-400 ml-2">{sig.description}</span>
                    )}
                  </div>
                  <span className={`font-semibold ${
                    sig.score > 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
                  }`}>
                    {sig.score > 0 ? '+' : ''}{sig.score}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* 歷史趨勢表格 */}
      {history.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-3 border-b border-gray-200 dark:border-gray-700 pb-2">
            歷史趨勢（最近 {history.length} 天）
          </h3>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
              <thead>
                <tr>
                  <th className="px-3 py-2 text-left text-xs text-gray-500 dark:text-gray-400">日期</th>
                  <th className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">CB 收盤</th>
                  <th className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">標的股</th>
                  <th className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">轉換價值</th>
                  <th className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">溢價率</th>
                  <th className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">成交量</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                {history.slice(-30).reverse().map((pt, idx) => (
                  <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="px-3 py-1.5 text-gray-600 dark:text-gray-400">
                      {new Date(pt.date).toLocaleDateString('zh-TW')}
                    </td>
                    <td className="px-3 py-1.5 text-right text-gray-900 dark:text-gray-100">
                      {pt.close != null ? pt.close.toFixed(2) : '-'}
                    </td>
                    <td className="px-3 py-1.5 text-right text-gray-900 dark:text-gray-100">
                      {pt.underlying_close != null ? pt.underlying_close.toFixed(2) : '-'}
                    </td>
                    <td className="px-3 py-1.5 text-right text-gray-900 dark:text-gray-100">
                      {pt.conversion_value != null ? pt.conversion_value.toFixed(2) : '-'}
                    </td>
                    <td className={`px-3 py-1.5 text-right ${
                      pt.premium_rate != null && pt.premium_rate < 0
                        ? 'text-green-600 dark:text-green-400'
                        : 'text-gray-600 dark:text-gray-400'
                    }`}>
                      {pt.premium_rate != null ? `${pt.premium_rate.toFixed(2)}%` : '-'}
                    </td>
                    <td className="px-3 py-1.5 text-right text-gray-900 dark:text-gray-100">
                      {pt.volume != null ? Math.round(pt.volume).toLocaleString() : '-'}
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

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-gray-500 dark:text-gray-400">{label}</span>
      <span className="text-gray-900 dark:text-gray-100">{value}</span>
    </div>
  )
}
