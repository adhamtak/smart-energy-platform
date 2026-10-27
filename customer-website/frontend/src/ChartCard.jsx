import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'

const COLORS = { bar: '#2563eb', kw: '#ea580c', voltage: '#16a34a', pf: '#7c3aed' }

function Card({ title, children }) {
  return (
    <div className="card chart">
      <h3>{title}</h3>
      <div className="chart-box">
        <ResponsiveContainer width="100%" height="100%">
          {children}
        </ResponsiveContainer>
      </div>
    </div>
  )
}

// "2026-07-01T14:15" -> "14:15" (one day) or "07-01 14:15" (many days)
const makeTick = (multiDay) => (t) => (multiDay ? t.slice(5, 16).replace('T', ' ') : t.slice(11, 16))

export function BarCard({ title, data, x, y, unit, multiDay }) {
  const tick = x === 't' ? makeTick(multiDay) : (v) => v.slice(5)
  return (
    <Card title={title}>
      <BarChart data={data} margin={{ top: 8, right: 8, left: -8, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey={x} tickFormatter={tick} minTickGap={32} fontSize={12} />
        <YAxis fontSize={12} />
        <Tooltip formatter={(v) => [`${v} ${unit}`, title]} />
        <Bar dataKey={y} fill={COLORS.bar} />
      </BarChart>
    </Card>
  )
}

export function LineCard({ title, data, y, unit, color, multiDay, domain, referenceY }) {
  return (
    <Card title={title}>
      <LineChart data={data} margin={{ top: 8, right: 8, left: -8, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="t" tickFormatter={makeTick(multiDay)} minTickGap={32} fontSize={12} />
        <YAxis fontSize={12} domain={domain || ['auto', 'auto']} />
        <Tooltip formatter={(v) => [`${v} ${unit}`, title]} />
        {referenceY && <ReferenceLine y={referenceY} stroke="#dc2626" strokeDasharray="4 4" />}
        <Line type="monotone" dataKey={y} stroke={COLORS[color]} dot={false} strokeWidth={2} />
      </LineChart>
    </Card>
  )
}
