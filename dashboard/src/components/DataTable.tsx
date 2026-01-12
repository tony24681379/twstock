/**
 * 通用數據表格組件
 * 支持泛型，可重用於不同類型的數據表格
 */

import React from 'react'

export interface ColumnDef<T> {
  key: string
  label: string
  align?: 'left' | 'right' | 'center'
  render?: (row: T, index: number) => React.ReactNode
  headerClassName?: string
  cellClassName?: string
}

interface DataTableProps<T> {
  data: T[]
  columns: ColumnDef<T>[]
  keyExtractor: (row: T, index: number) => string | number
  emptyMessage?: string
  className?: string
  darkMode?: boolean
}

/**
 * 通用數據表格組件
 */
export function DataTable<T>({
  data,
  columns,
  keyExtractor,
  emptyMessage = '無資料',
  className = '',
}: DataTableProps<T>) {
  if (data.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
        {emptyMessage}
      </div>
    )
  }

  const getAlignClass = (align?: 'left' | 'right' | 'center') => {
    switch (align) {
      case 'right':
        return 'text-right'
      case 'center':
        return 'text-center'
      default:
        return 'text-left'
    }
  }

  return (
    <div className={`overflow-x-auto ${className}`}>
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-900">
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                className={`px-6 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase ${getAlignClass(column.align)} ${column.headerClassName || ''}`}
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          {data.map((row, index) => (
            <tr
              key={keyExtractor(row, index)}
              className="hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            >
              {columns.map((column) => (
                <td
                  key={column.key}
                  className={`px-6 py-4 whitespace-nowrap text-sm ${getAlignClass(column.align)} ${column.cellClassName || ''}`}
                >
                  {column.render ? column.render(row, index) : (row as any)[column.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
