import React from 'react'
import { AlertStatus } from '../types/stock'

interface AlertDetailCardProps {
  alertStatus?: AlertStatus
  stockId: string
}

export const AlertDetailCard: React.FC<AlertDetailCardProps> = ({ alertStatus, stockId }) => {
  if (!alertStatus || (!alertStatus.is_disposal_stock && !alertStatus.is_attention_stock)) {
    return null
  }

  // 處置股卡片
  if (alertStatus.is_disposal_stock) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border-2 border-red-500 rounded-lg p-4 mb-6">
        <div className="flex items-center mb-3">
          <span className="text-2xl mr-2">🚫</span>
          <h3 className="text-lg font-semibold text-red-700 dark:text-red-300">處置股警示</h3>
        </div>
        <div className="space-y-2 text-sm text-gray-700 dark:text-gray-300">
          {alertStatus.disposal_type && (
            <p><strong>處置類型：</strong>{alertStatus.disposal_type}</p>
          )}
          {(alertStatus.disposal_start_date || alertStatus.disposal_end_date) && (
            <p>
              <strong>處置期間：</strong>
              {alertStatus.disposal_start_date || '未知'} ~ {alertStatus.disposal_end_date || '未知'}
            </p>
          )}
          {alertStatus.disposal_condition && (
            <p><strong>處置條件：</strong>{alertStatus.disposal_condition}</p>
          )}
          {alertStatus.disposal_measure && (
            <p><strong>處置措施：</strong>{alertStatus.disposal_measure}</p>
          )}
          {alertStatus.disposal_content && (
            <p className="text-xs"><strong>處置內容：</strong>{alertStatus.disposal_content}</p>
          )}
          <div className="mt-3 p-3 bg-red-100 dark:bg-red-900/30 rounded">
            <p className="text-red-800 dark:text-red-200">
              💡 <strong>投資提醒：</strong>處置股交易受限，請謹慎評估風險
            </p>
          </div>
          <a
            href={`https://www.twse.com.tw/zh/announcement/punish.html?stockNo=${stockId}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block mt-2 text-red-600 dark:text-red-400 hover:underline"
          >
            📄 查看證交所公告 →
          </a>
        </div>
      </div>
    )
  }

  // 注意股卡片
  if (alertStatus.is_attention_stock) {
    return (
      <div className="bg-orange-50 dark:bg-orange-900/20 border-2 border-orange-500 rounded-lg p-4 mb-6">
        <div className="flex items-center mb-3">
          <span className="text-2xl mr-2">⚠️</span>
          <h3 className="text-lg font-semibold text-orange-700 dark:text-orange-300">注意股警示</h3>
        </div>
        <div className="space-y-2 text-sm text-gray-700 dark:text-gray-300">
          {alertStatus.attention_count && (
            <p><strong>累計次數：</strong>{alertStatus.attention_count} 次</p>
          )}
          {alertStatus.attention_date && (
            <p><strong>最近日期：</strong>{alertStatus.attention_date}</p>
          )}
          {alertStatus.attention_reason && (
            <p><strong>注意原因：</strong>{alertStatus.attention_reason}</p>
          )}
          <div className="mt-3 p-3 bg-orange-100 dark:bg-orange-900/30 rounded">
            <p className="text-orange-800 dark:text-orange-200">
              💡 <strong>投資提醒：</strong>該股票交易異常，請留意風險
            </p>
          </div>
          <a
            href={`https://www.twse.com.tw/zh/announcement/notice.html?stockNo=${stockId}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block mt-2 text-orange-600 dark:text-orange-400 hover:underline"
          >
            📄 查看證交所公告 →
          </a>
        </div>
      </div>
    )
  }

  return null
}
