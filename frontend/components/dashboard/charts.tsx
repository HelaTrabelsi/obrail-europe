'use client'

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
  Area,
  AreaChart,
  ScatterChart,
  Scatter,
  Legend,
} from 'recharts'

const COLORS = {
  primary: '#00c98d',
  accent: '#0096d6',
  warning: '#f59e0b',
  purple: '#8b5cf6',
  destructive: '#ef4444',
  emerald: '#34d399',
  sky: '#60a5fa',
}

const chartColors = [
  COLORS.primary,
  COLORS.accent,
  COLORS.warning,
  COLORS.purple,
  COLORS.destructive,
  COLORS.emerald,
  COLORS.sky,
]

const axisStyle = {
  fontSize: 10,
  fill: '#354d62',
  fontFamily: 'Inter, system-ui, sans-serif',
}

const tooltipStyle = {
  backgroundColor: '#111c28',
  border: '1px solid rgba(255,255,255,0.08)',
  borderRadius: '8px',
  fontSize: '12px',
  color: '#9eb8cc',
}

interface BarChartComponentProps {
  data: { name: string; value: number; [key: string]: unknown }[]
  dataKey?: string
  xAxisKey?: string
  color?: string
  height?: number
  horizontal?: boolean
  showGrid?: boolean
}

export function BarChartComponent({
  data,
  dataKey = 'value',
  xAxisKey = 'name',
  color = COLORS.primary,
  height = 300,
  horizontal = false,
  showGrid = false,
}: BarChartComponentProps) {
  if (horizontal) {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} layout="vertical" margin={{ left: 80, right: 20, top: 10, bottom: 10 }}>
          {showGrid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" horizontal />}
          <XAxis type="number" tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
          <YAxis dataKey={xAxisKey} type="category" tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} width={75} />
          <Tooltip contentStyle={tooltipStyle} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
          <Bar dataKey={dataKey} fill={color} radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ left: 10, right: 10, top: 10, bottom: 10 }}>
        {showGrid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />}
        <XAxis dataKey={xAxisKey} tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
        <YAxis tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
        <Tooltip contentStyle={tooltipStyle} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
        <Bar dataKey={dataKey} fill={color} radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

interface GroupedBarChartProps {
  data: { name: string; [key: string]: unknown }[]
  keys: { key: string; color: string; label: string }[]
  xAxisKey?: string
  height?: number
}

export function GroupedBarChart({
  data,
  keys,
  xAxisKey = 'name',
  height = 300,
}: GroupedBarChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ left: 10, right: 10, top: 10, bottom: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
        <XAxis dataKey={xAxisKey} tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
        <YAxis tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
        <Tooltip contentStyle={tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: '11px', color: '#4a6275' }} />
        {keys.map(({ key, color, label }) => (
          <Bar key={key} dataKey={key} fill={color} name={label} radius={[4, 4, 0, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

interface DonutChartProps {
  data: { name: string; value: number }[]
  colors?: string[]
  height?: number
  innerRadius?: number
  outerRadius?: number
}

export function DonutChart({
  data,
  colors = chartColors,
  height = 300,
  innerRadius = 60,
  outerRadius = 100,
}: DonutChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          innerRadius={innerRadius}
          outerRadius={outerRadius}
          paddingAngle={2}
          dataKey="value"
          label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
          labelLine={false}
        >
          {data.map((_, index) => (
            <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
          ))}
        </Pie>
        <Tooltip contentStyle={tooltipStyle} />
      </PieChart>
    </ResponsiveContainer>
  )
}

interface HistogramProps {
  data: { name: string | number; value: number }[]
  color?: string
  height?: number
  xLabel?: string
}

export function Histogram({
  data,
  color = COLORS.primary,
  height = 200,
  xLabel,
}: HistogramProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ left: 10, right: 10, top: 10, bottom: 10 }}>
        <defs>
          <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.3} />
            <stop offset="95%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <XAxis 
          dataKey="name" 
          tick={axisStyle} 
          axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} 
          tickLine={{ stroke: 'rgba(255,255,255,0.06)' }}
          label={xLabel ? { value: xLabel, position: 'bottom', style: axisStyle } : undefined}
        />
        <YAxis tick={axisStyle} axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} tickLine={{ stroke: 'rgba(255,255,255,0.06)' }} />
        <Tooltip contentStyle={tooltipStyle} />
        <Area type="monotone" dataKey="value" stroke={color} fillOpacity={1} fill="url(#colorValue)" />
      </AreaChart>
    </ResponsiveContainer>
  )
}

interface ScatterPlotProps {
  data: { x: number; y: number; category?: string }[]
  height?: number
  xLabel?: string
  yLabel?: string
}

export function ScatterPlot({
  data,
  height = 300,
  xLabel,
  yLabel,
}: ScatterPlotProps) {
  const categories = [...new Set(data.map(d => d.category || 'default'))]
  const categoryColors: Record<string, string> = {
    'Jour': COLORS.warning,
    'Nuit': COLORS.purple,
    'default': COLORS.primary,
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <ScatterChart margin={{ left: 10, right: 10, top: 10, bottom: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
        <XAxis 
          dataKey="x" 
          type="number"
          tick={axisStyle} 
          axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} 
          tickLine={{ stroke: 'rgba(255,255,255,0.06)' }}
          name={xLabel}
        />
        <YAxis 
          dataKey="y" 
          type="number"
          tick={axisStyle} 
          axisLine={{ stroke: 'rgba(255,255,255,0.06)' }} 
          tickLine={{ stroke: 'rgba(255,255,255,0.06)' }}
          name={yLabel}
        />
        <Tooltip contentStyle={tooltipStyle} cursor={{ strokeDasharray: '3 3' }} />
        <Legend wrapperStyle={{ fontSize: '11px', color: '#4a6275' }} />
        {categories.map((cat) => (
          <Scatter
            key={cat}
            name={cat}
            data={data.filter(d => (d.category || 'default') === cat)}
            fill={categoryColors[cat] || COLORS.primary}
            opacity={0.4}
          />
        ))}
      </ScatterChart>
    </ResponsiveContainer>
  )
}

export { COLORS }
