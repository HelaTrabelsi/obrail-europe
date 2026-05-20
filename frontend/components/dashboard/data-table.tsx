'use client'

import { cn } from '@/lib/utils'

interface DataTableProps {
  columns: { key: string; label: string; align?: 'left' | 'center' | 'right' }[]
  data: Record<string, unknown>[]
  className?: string
  maxHeight?: string
}

export function DataTable({ columns, data, className, maxHeight = '360px' }: DataTableProps) {
  return (
    <div className={cn("rounded-lg border border-border/50 overflow-hidden", className)}>
      <div className="overflow-auto" style={{ maxHeight }}>
        <table className="w-full text-sm">
          <thead className="bg-card sticky top-0">
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  className={cn(
                    "px-4 py-3 text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60 border-b border-border/50",
                    col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'
                  )}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-border/30">
            {data.map((row, i) => (
              <tr key={i} className="bg-card/50 hover:bg-card transition-colors">
                {columns.map((col) => (
                  <td
                    key={col.key}
                    className={cn(
                      "px-4 py-3 text-secondary-foreground",
                      col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'
                    )}
                  >
                    {String(row[col.key] ?? '')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
