import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceArea, ReferenceLine,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'

export const COLORS = {
  kwh: '#2563EB',
  kw: '#0891B2',
  voltage: '#1E3A8A',
  pf: '#60A5FA',
  danger: '#B3261E',
}

const AXIS_TICK = { fill: '#5E6E80', fontSize: 12 }
const MARGIN = { top: 8, right: 6, left: 0, bottom: 0 }

// "2026-07-01T14:15" -> "14:15" (one day) or "07-01 14:15" (many days)
const timeTick = (multiDay) => (t) => (multiDay ? t.slice(5, 16).replace('T', ' ') : t.slice(11, 16))
const dateTick = (v) => v.slice(5)
const fullTime = (t) => String(t).slice(0, 16).replace('T', ' ')

function Tip({ active, payload, label, unit, digits = 2 }) {
  if (!active || !payload?.length) return null
  const v = payload[0].value
  return (
    <div className="tip">
      <div className="tip-label">{fullTime(label)}</div>
      <div className="tip-value">
        {v == null ? '–' : Number(v).toLocaleString(undefined, { maximumFractionDigits: digits })}
        {unit && <span> {unit}</span>}
      </div>
    </div>
  )
}

const grid = <CartesianGrid key="grid" vertical={false} stroke="#E1E8F2" strokeDasharray="2 4" />
const xAxis = (key, tickFormatter) => (
  <XAxis key="x" dataKey={key} tickFormatter={tickFormatter} minTickGap={36} tick={AXIS_TICK} tickLine={false} axisLine={{ stroke: '#DCE2E9' }} tickMargin={8} />
)
const yAxis = (props) => <YAxis key="y" tick={AXIS_TICK} tickLine={false} axisLine={false} width={44} {...props} />

export function Panel({ title, note, action, children }) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h3>{title}</h3>
          {note && <p className="note">{note}</p>}
        </div>
        {action}
      </header>
      <div className="plot">
        <ResponsiveContainer width="100%" height="100%">
          {children}
        </ResponsiveContainer>
      </div>
    </section>
  )
}

function Gradient({ id, color }) {
  return (
    <defs>
      <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={color} stopOpacity={0.28} />
        <stop offset="100%" stopColor={color} stopOpacity={0} />
      </linearGradient>
    </defs>
  )
}

// Wrapped in <Panel>: each returns a single recharts chart element.
export function consumptionChart({ view, hourly, daily, readings, multiDay }) {
  if (view === 'load') {
    return (
      <AreaChart data={readings} margin={MARGIN}>
        <Gradient id="g-kw" color={COLORS.kw} />
        {grid}
        {xAxis('t', timeTick(multiDay))}
        {yAxis()}
        <Tooltip content={<Tip unit="kW" digits={3} />} cursor={{ stroke: '#9AA8B8' }} />
        <Area type="monotone" dataKey="kw" stroke={COLORS.kw} strokeWidth={2} fill="url(#g-kw)" dot={false} activeDot={{ r: 4 }} />
      </AreaChart>
    )
  }
  const isDay = view === 'day'
  return (
    <BarChart data={isDay ? daily : hourly} margin={MARGIN} barCategoryGap={isDay ? '22%' : '12%'}>
      {grid}
      {xAxis(isDay ? 'date' : 't', isDay ? dateTick : timeTick(multiDay))}
      {yAxis()}
      <Tooltip content={<Tip unit="kWh" />} cursor={{ fill: 'rgba(37,99,235,.08)' }} />
      <Bar dataKey="kwh" fill={COLORS.kwh} radius={[3, 3, 0, 0]} />
    </BarChart>
  )
}

export function voltageChart({ readings, threshold, multiDay, limitLabel }) {
  return (
    <LineChart data={readings} margin={MARGIN}>
      {grid}
      {xAxis('t', timeTick(multiDay))}
      {yAxis({ domain: [(min) => Math.floor(Math.min(min, threshold) - 5), 'auto'] })}
      <ReferenceArea y2={threshold} fill={COLORS.danger} fillOpacity={0.07} />
      <ReferenceLine
        y={threshold}
        stroke={COLORS.danger}
        strokeDasharray="4 4"
        label={{ value: limitLabel ?? `${threshold} V`, position: 'insideTopRight', fill: COLORS.danger, fontSize: 11 }}
      />
      <Tooltip content={<Tip unit="V" digits={1} />} cursor={{ stroke: '#9AA8B8' }} />
      <Line type="monotone" dataKey="voltage" stroke={COLORS.voltage} strokeWidth={2} dot={false} activeDot={{ r: 4 }} />
    </LineChart>
  )
}

export function powerFactorChart({ readings, multiDay }) {
  return (
    <AreaChart data={readings} margin={MARGIN}>
      <Gradient id="g-pf" color={COLORS.pf} />
      {grid}
      {xAxis('t', timeTick(multiDay))}
      {yAxis({ domain: [0, 1], ticks: [0, 0.25, 0.5, 0.75, 1] })}
      <Tooltip content={<Tip digits={3} />} cursor={{ stroke: '#9AA8B8' }} />
      <Area type="monotone" dataKey="pf" stroke={COLORS.pf} strokeWidth={2} fill="url(#g-pf)" dot={false} activeDot={{ r: 4 }} />
    </AreaChart>
  )
}
