import { useState } from 'react'
import type { ChipSignal } from '../types/stock'

interface Props {
  technical_signals: ChipSignal[]
  recent_events?: ChipSignal[]
  technical_strength: number
  className?: string
}

type TabType = 'all' | 'ma' | 'momentum' | 'pattern' | 'trend' | 'events' | 'other'

export function TechnicalCard({ technical_signals, recent_events = [], technical_strength, className = '' }: Props) {
  const [activeTab, setActiveTab] = useState<TabType>('all')

  // 訊號分類
  const categorizeSignals = () => {
    const categories: Record<TabType, ChipSignal[]> = {
      all: technical_signals,
      ma: [],
      momentum: [],
      pattern: [],
      trend: [],
      events: recent_events,
      other: []
    }

    technical_signals.forEach(signal => {
      const name = signal.name.toLowerCase()

      // 均線訊號
      if (name.includes('三線合一') || name.includes('四線合一') ||
          name.includes('黃金交叉') || name.includes('死亡交叉') ||
          name.includes('季線') || name.includes('均線')) {
        categories.ma.push(signal)
      }
      // 動量訊號
      else if (name.includes('kd') || name.includes('macd') ||
               name.includes('dmi') || name.includes('rsi')) {
        categories.momentum.push(signal)
      }
      // 形態訊號
      else if (name.includes('長紅') || name.includes('長黑') ||
               name.includes('吞噬') || name.includes('跳空') ||
               name.includes('布林') || name.includes('突破')) {
        categories.pattern.push(signal)
      }
      // 趨勢訊號
      else if (name.includes('多頭') || name.includes('空頭') ||
               name.includes('排列') || name.includes('新高') ||
               name.includes('新低')) {
        categories.trend.push(signal)
      }
      // 其他訊號
      else {
        categories.other.push(signal)
      }
    })

    return categories
  }

  const categories = categorizeSignals()

  // Tab 配置
  const tabs = [
    { id: 'all' as TabType, label: '全部訊號', count: categories.all.length },
    { id: 'ma' as TabType, label: '均線訊號', count: categories.ma.length },
    { id: 'momentum' as TabType, label: '動量訊號', count: categories.momentum.length },
    { id: 'pattern' as TabType, label: '形態訊號', count: categories.pattern.length },
    { id: 'trend' as TabType, label: '趨勢訊號', count: categories.trend.length },
    { id: 'events' as TabType, label: '近期事件', count: categories.events.length },
    { id: 'other' as TabType, label: '其他訊號', count: categories.other.length },
  ]

  // 強度徽章顏色
  const getStrengthBadgeClass = (strength: number) => {
    if (strength >= 70) return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    if (strength >= 50) return 'bg-green-50 text-green-700 dark:bg-green-900/50 dark:text-green-300'
    if (strength >= 20) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
    if (strength >= 0) return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
    if (strength >= -20) return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
    return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
  }

  // 訊號顏色類別
  const getSignalColorClass = (score: number) => {
    if (score > 0) {
      return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
    }
    return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
  }

  // 事件訊號使用不同樣式
  const getEventColorClass = (score: number) => {
    if (score > 0) {
      return 'bg-amber-50 text-amber-800 border border-amber-300 dark:bg-amber-900/30 dark:text-amber-200 dark:border-amber-700'
    }
    return 'bg-purple-50 text-purple-800 border border-purple-300 dark:bg-purple-900/30 dark:text-purple-200 dark:border-purple-700'
  }

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow ${className}`}>
      {/* 標題與強度分數 */}
      <div className="flex items-center justify-between px-6 pt-6 pb-4">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
          技術分析
        </h2>
        <div className={`px-4 py-2 rounded-full text-sm font-bold ${getStrengthBadgeClass(technical_strength)}`}>
          {technical_strength > 0 ? '+' : ''}{technical_strength}
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
              {tab.label}
              {tab.count > 0 && (
                <span className={`ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                  tab.id === 'events'
                    ? 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200'
                    : 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
                }`}>
                  {tab.count}
                </span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab 內容區域 */}
      <div className="p-6">
        {activeTab === 'events' ? (
          <SignalsList
            signals={categories.events}
            getSignalColorClass={getEventColorClass}
          />
        ) : (
          <SignalsList
            signals={categories[activeTab]}
            getSignalColorClass={getSignalColorClass}
          />
        )}
      </div>
    </div>
  )
}

// 訊號列表元件
function SignalsList({
  signals,
  getSignalColorClass
}: {
  signals: ChipSignal[]
  getSignalColorClass: (score: number) => string
}) {
  if (signals.length === 0) {
    return (
      <div className="text-center py-8">
        <div className="text-sm text-gray-500 dark:text-gray-400">
          無觸發訊號
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-2">
      {signals.map((signal, index) => (
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
  )
}
