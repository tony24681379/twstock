import { useEffect, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ConvertibleBondHistoryPoint } from '../types/stock'

interface CBHistoryChartProps {
  history: ConvertibleBondHistoryPoint[]
}

interface ChartRow {
  date: string
  close: number | null
  conversionValue: number | null
  premiumRate: number | null
  volume: number | null
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr)
  return `${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getDate().toString().padStart(2, '0')}`
}

export default function CBHistoryChart({ history }: CBHistoryChartProps) {
  const [darkMode, setDarkMode] = useState(
    document.documentElement.classList.contains('dark')
  )

  useEffect(() => {
    const observer = new MutationObserver(() => {
      setDarkMode(document.documentElement.classList.contains('dark'))
    })
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    return () => observer.disconnect()
  }, [])

  const gridColor = darkMode ? '#374151' : '#E5E7EB'
  const textColor = darkMode ? '#9CA3AF' : '#6B7280'
  const tooltipBg = darkMode ? '#1F2937' : '#FFFFFF'
  const tooltipBorder = darkMode ? '#374151' : '#E5E7EB'
  const tooltipText = darkMode ? '#F3F4F6' : '#111827'

  const data: ChartRow[] = history.map((pt) => ({
    date: formatDate(pt.date),
    close: pt.close,
    conversionValue: pt.conversion_value,
    premiumRate: pt.premium_rate != null ? Number(pt.premium_rate.toFixed(2)) : null,
    volume: pt.volume != null ? Math.round(pt.volume) : null,
  }))

  return (
    <div className="space-y-1">
      {/* Panel 1: CB 收盤 vs 轉換價值 */}
      <ResponsiveContainer width="100%" height={250}>
        <LineChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
          <XAxis dataKey="date" stroke={textColor} tick={{ fontSize: 11 }} hide />
          <YAxis
            stroke={textColor}
            tick={{ fontSize: 11 }}
            domain={['auto', 'auto']}
            tickFormatter={(v: number) => v.toFixed(0)}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: tooltipBg,
              border: `1px solid ${tooltipBorder}`,
              color: tooltipText,
              fontSize: 12,
            }}
            formatter={(value: number, name: string) => {
              const label = name === 'close' ? 'CB 收盤' : '轉換價值'
              return [value != null ? value.toFixed(2) : '-', label]
            }}
            labelFormatter={(label: string) => `日期: ${label}`}
          />
          <Legend
            formatter={(value: string) => (value === 'close' ? 'CB 收盤' : '轉換價值')}
            wrapperStyle={{ fontSize: 12, color: textColor }}
          />
          <Line
            type="monotone"
            dataKey="close"
            stroke="#3B82F6"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="conversionValue"
            stroke="#F97316"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>

      {/* Panel 2: 溢價率 */}
      <ResponsiveContainer width="100%" height={150}>
        <BarChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
          <XAxis dataKey="date" stroke={textColor} tick={{ fontSize: 11 }} hide />
          <YAxis
            stroke={textColor}
            tick={{ fontSize: 11 }}
            tickFormatter={(v: number) => `${v}%`}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: tooltipBg,
              border: `1px solid ${tooltipBorder}`,
              color: tooltipText,
              fontSize: 12,
            }}
            formatter={(value: number) => [value != null ? `${value.toFixed(2)}%` : '-', '溢價率']}
            labelFormatter={(label: string) => `日期: ${label}`}
          />
          <ReferenceLine y={0} stroke={textColor} strokeDasharray="4 4" />
          <Bar dataKey="premiumRate" name="溢價率">
            {data.map((entry, index) => (
              <Cell
                key={index}
                fill={entry.premiumRate != null && entry.premiumRate < 0 ? '#10B981' : '#EF4444'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>

      {/* Panel 3: 成交量 */}
      <ResponsiveContainer width="100%" height={100}>
        <BarChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
          <XAxis
            dataKey="date"
            stroke={textColor}
            tick={{ fontSize: 10 }}
            interval="equidistantPreserveStart"
          />
          <YAxis
            stroke={textColor}
            tick={{ fontSize: 11 }}
            tickFormatter={(v: number) => (v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v))}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: tooltipBg,
              border: `1px solid ${tooltipBorder}`,
              color: tooltipText,
              fontSize: 12,
            }}
            formatter={(value: number) => [value != null ? value.toLocaleString() : '-', '成交量']}
            labelFormatter={(label: string) => `日期: ${label}`}
          />
          <Bar dataKey="volume" fill="#9CA3AF" name="成交量" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
