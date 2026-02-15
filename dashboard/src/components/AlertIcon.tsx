import React from 'react'
import { AlertStatus } from '../types/stock'

interface AlertIconProps {
  alertStatus?: AlertStatus
}

export const AlertIcon: React.FC<AlertIconProps> = ({ alertStatus }) => {
  if (!alertStatus || (!alertStatus.is_disposal_stock && !alertStatus.is_attention_stock)) {
    return null
  }

  const { is_disposal_stock, is_attention_stock, disposal_start_date, disposal_end_date, attention_date, attention_count } = alertStatus

  // 處置股優先顯示
  if (is_disposal_stock) {
    const title = `處置股\n處置期間：${disposal_start_date || '未知'} ~ ${disposal_end_date || '未知'}`
    return (
      <span
        className="ml-1 cursor-help text-red-600 dark:text-red-400"
        title={title}
        aria-label={title}
      >
        🚫
      </span>
    )
  }

  // 注意股
  if (is_attention_stock) {
    const title = `注意股\n累計次數：${attention_count || 1} 次\n最近日期：${attention_date || '未知'}`
    return (
      <span
        className="ml-1 cursor-help text-orange-500 dark:text-orange-400"
        title={title}
        aria-label={title}
      >
        ⚠️
      </span>
    )
  }

  return null
}
