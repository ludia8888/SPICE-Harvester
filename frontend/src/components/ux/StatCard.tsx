import { Tooltip } from '@blueprintjs/core'
import { AreaChart, Area, ResponsiveContainer } from 'recharts'

type StatCardProps = {
  label: string
  value: string | number
  tooltip?: string
  sparkData?: { v: number }[]
  color?: string
}

export const StatCard = ({
  label,
  value,
  tooltip,
  sparkData,
  color = '#137cbd',
}: StatCardProps) => {
  const content = (
    <div className="stat-card">
      <div className="stat-card-value">{value}</div>
      <div className="stat-card-label">
        {tooltip ? (
          <Tooltip content={tooltip} placement="top">
            <span className="tooltip-label">{label}</span>
          </Tooltip>
        ) : (
          label
        )}
      </div>
      {sparkData && sparkData.length > 1 && (
        <div className="stat-card-spark">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={sparkData}>
              <defs>
                <linearGradient id={`spark-${label}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity={0.3} />
                  <stop offset="100%" stopColor={color} stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <Area
                type="monotone"
                dataKey="v"
                stroke={color}
                strokeWidth={1.5}
                fill={`url(#spark-${label})`}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
  return content
}
