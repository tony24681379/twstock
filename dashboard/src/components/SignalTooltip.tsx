import { ChipSignal } from '../types/stock'

interface SignalTooltipProps {
  title: string
  signals: ChipSignal[]
  show: boolean
}

export default function SignalTooltip({ title, signals, show }: SignalTooltipProps) {
  if (!show || !signals || signals.length === 0) {
    return null
  }

  return (
    <div className="hidden group-hover:block absolute top-full left-1/2 -translate-x-1/2 mt-2
                    bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900
                    rounded-lg px-3 py-2 text-xs shadow-xl z-50 min-w-[300px]
                    before:content-[''] before:absolute before:bottom-full before:left-1/2 before:-translate-x-1/2
                    before:border-4 before:border-transparent before:border-b-gray-900 dark:before:border-b-gray-100">

      {/* 標題 */}
      <div className="font-semibold mb-1.5 border-b border-gray-700 dark:border-gray-300 pb-1">
        {title}
      </div>

      {/* 訊號列表 */}
      <div className="space-y-0.5 max-h-[400px] overflow-y-auto">
        {signals.map((signal, idx) => (
          <div key={idx} className="flex justify-between gap-3">
            <span className="text-left">• {signal.name}</span>
            <span className={`font-semibold ${
              signal.score > 0
                ? 'text-green-400 dark:text-green-600'
                : 'text-red-400 dark:text-red-600'
            }`}>
              {signal.score > 0 ? '+' : ''}{signal.score}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}
