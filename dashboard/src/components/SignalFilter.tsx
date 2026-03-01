import { useState, useMemo } from 'react'
import { SIGNAL_CATALOG } from '../constants/signalCatalog'
import type { StockListItem } from '../types/stock'

interface SignalFilterProps {
  activeSignals: string[]
  onSignalsChange: (signals: string[]) => void
  stocks: StockListItem[]
  className?: string
}

export function SignalFilter({ activeSignals, onSignalsChange, stocks, className = '' }: SignalFilterProps) {
  const [expanded, setExpanded] = useState(false)

  // 計算每個訊號的觸發股票數
  const signalStockCounts = useMemo(() => {
    const counts: Record<string, number> = {}
    for (const stock of stocks) {
      const allSignals = [
        ...(stock.chip_signals || []),
        ...(stock.technical_signals || []),
        ...(stock.fundamental_signals || []),
        ...(stock.recent_events || []),
        ...(stock.cb_signals || []),
      ]
      for (const sig of allSignals) {
        counts[sig.name] = (counts[sig.name] || 0) + 1
      }
    }
    return counts
  }, [stocks])

  const toggleSignal = (name: string) => {
    if (activeSignals.includes(name)) {
      onSignalsChange(activeSignals.filter(s => s !== name))
    } else {
      onSignalsChange([...activeSignals, name])
    }
  }

  const clearAll = () => onSignalsChange([])

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow ${className}`}>
      {/* Header - 點擊展開/收合 */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between p-4 text-left"
      >
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            訊號篩選
          </h3>
          {activeSignals.length > 0 && (
            <span className="inline-flex items-center justify-center w-5 h-5 text-xs font-bold text-white bg-blue-600 rounded-full">
              {activeSignals.length}
            </span>
          )}
        </div>
        <svg
          className={`w-4 h-4 text-gray-500 dark:text-gray-400 transition-transform ${expanded ? 'rotate-180' : ''}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* 展開內容 */}
      {expanded && (
        <div className="px-4 pb-4 border-t border-gray-200 dark:border-gray-700">
          {/* 已選標籤列 */}
          {activeSignals.length > 0 && (
            <div className="flex flex-wrap items-center gap-1.5 pt-3 pb-2">
              {activeSignals.map(name => (
                <button
                  key={name}
                  onClick={() => toggleSignal(name)}
                  className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium
                    bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200
                    hover:bg-blue-200 dark:hover:bg-blue-800 transition-colors"
                >
                  {name}
                  <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              ))}
              <button
                onClick={clearAll}
                className="text-xs text-gray-500 dark:text-gray-400 hover:text-red-600 dark:hover:text-red-400 ml-1"
              >
                清除全部
              </button>
            </div>
          )}

          {/* 分類標籤區 */}
          <div className="space-y-3 pt-2">
            {SIGNAL_CATALOG.map(category => (
              <div key={category.key}>
                <div className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1.5">
                  {category.label}
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {category.signals.map(signal => {
                    const count = signalStockCounts[signal.name] || 0
                    const isActive = activeSignals.includes(signal.name)
                    const isPositive = signal.score > 0
                    const isDisabled = count === 0

                    let tagClass: string
                    if (isActive) {
                      tagClass = isPositive
                        ? 'bg-green-600 text-white dark:bg-green-500'
                        : 'bg-red-600 text-white dark:bg-red-500'
                    } else if (isDisabled) {
                      tagClass = 'bg-gray-100 text-gray-400 dark:bg-gray-700 dark:text-gray-500 cursor-not-allowed'
                    } else {
                      tagClass = isPositive
                        ? 'bg-green-50 text-green-700 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-400 dark:hover:bg-green-900/50'
                        : 'bg-red-50 text-red-700 hover:bg-red-100 dark:bg-red-900/30 dark:text-red-400 dark:hover:bg-red-900/50'
                    }

                    return (
                      <button
                        key={signal.name}
                        onClick={() => !isDisabled && toggleSignal(signal.name)}
                        disabled={isDisabled}
                        className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium transition-colors ${tagClass}`}
                      >
                        {signal.name}
                        <span className={`${isActive ? 'opacity-75' : 'opacity-50'}`}>
                          {count}
                        </span>
                      </button>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
