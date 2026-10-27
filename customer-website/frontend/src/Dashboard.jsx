import { useEffect, useState } from 'react'
import { api } from './api.js'
import { BarCard, LineCard } from './ChartCard.jsx'

const METER_RE = /^MTR\d{6}$/
const fmt = (v, digits = 1) => (v == null ? '–' : Number(v).toLocaleString(undefined, { maximumFractionDigits: digits }))
const when = (v) => (v ? String(v).replace('T', ' ').slice(0, 16) : '–')

function Kpi({ label, value, unit, sub }) {
  return (
    <div className="card kpi">
      <div className="muted small">{label}</div>
      <div className="kpi-value">
        {value} <span className="unit">{unit}</span>
      </div>
      {sub && <div className="muted small">{sub}</div>}
    </div>
  )
}

export default function Dashboard({ user, onSignOut }) {
  const defaultMeter = user.is_admin ? 'MTR000001' : user.meters[0]
  const [input, setInput] = useState(defaultMeter)
  const [meterId, setMeterId] = useState(defaultMeter)
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError('')
    api
      .meter(meterId)
      .then((d) => !cancelled && setData(d))
      .catch((e) => {
        if (cancelled) return
        if (e.status === 401) return onSignOut() // session expired
        setData(null)
        setError(e.message)
      })
      .finally(() => !cancelled && setLoading(false))
    return () => {
      cancelled = true
    }
  }, [meterId]) // eslint-disable-line react-hooks/exhaustive-deps

  const submit = (event) => {
    event.preventDefault()
    const id = input.trim().toUpperCase()
    if (!METER_RE.test(id)) return setError('Meter IDs look like MTR000001')
    setMeterId(id)
  }

  const s = data?.summary
  const multiDay = s?.multi_day ?? false

  return (
    <div className="page">
      <header>
        <h1>⚡ Smart Energy</h1>
        <div className="right">
          <span className="muted">
            {user.username}
            {user.is_admin && ' (admin)'}
          </span>
          <button className="ghost" onClick={onSignOut}>
            Sign out
          </button>
        </div>
      </header>

      <form className="meter-bar" onSubmit={submit}>
        {user.is_admin ? (
          <input value={input} onChange={(e) => setInput(e.target.value)} placeholder="MTR000001" />
        ) : (
          <select value={input} onChange={(e) => setInput(e.target.value)}>
            {user.meters.map((m) => (
              <option key={m}>{m}</option>
            ))}
          </select>
        )}
        <button disabled={loading}>{loading ? 'Loading…' : 'Show meter'}</button>
      </form>

      {error && <div className="alert error">{error}</div>}

      {data && (
        <>
          <p className="muted">
            <strong>{data.meter_id}</strong>
            {data.info.customer_type && ` · ${data.info.customer_type}`}
            {data.info.phase_type && ` · ${data.info.phase_type}`}
            {data.info.transformer_id && ` · transformer ${data.info.transformer_id}`}
            {data.info.status && ` · ${data.info.status}`} · times in {data.timezone}
          </p>

          {s.low_voltage_intervals > 0 && (
            <div className="alert warn">
              Voltage was below {data.low_voltage_threshold} V in {s.low_voltage_intervals} of {s.n_readings} readings.
            </div>
          )}

          <div className="kpis">
            <Kpi label="Total consumption" value={fmt(s.total_kwh)} unit="kWh" />
            <Kpi label="Average per day" value={fmt(s.avg_daily_kwh)} unit="kWh" />
            <Kpi label="Peak load" value={fmt(s.peak_kw, 2)} unit="kW" sub={when(s.peak_time)} />
            <Kpi label="Lowest voltage" value={fmt(s.min_voltage)} unit="V" sub={`avg ${fmt(s.avg_voltage)} V`} />
            <Kpi label="Avg power factor" value={fmt(s.avg_power_factor, 2)} unit="" />
          </div>

          <div className="charts">
            <BarCard title="Consumption per hour" data={data.hourly} x="t" y="kwh" unit="kWh" multiDay={multiDay} />
            {data.daily.length > 0 ? (
              <BarCard title="Consumption per day" data={data.daily} x="date" y="kwh" unit="kWh" />
            ) : (
              <LineCard title="Load" data={data.readings} y="kw" unit="kW" color="kw" multiDay={multiDay} />
            )}
            <LineCard
              title="Voltage"
              data={data.readings}
              y="voltage"
              unit="V"
              color="voltage"
              multiDay={multiDay}
              domain={[(min) => Math.floor(Math.min(min, data.low_voltage_threshold) - 5), 'auto']}
              referenceY={data.low_voltage_threshold}
            />
            <LineCard title="Power factor" data={data.readings} y="pf" unit="" color="pf" multiDay={multiDay} domain={[0, 1]} />
          </div>

          <h2>Incidents on this meter or its transformer</h2>
          {data.incidents.length === 0 ? (
            <div className="card muted">No incidents recorded.</div>
          ) : (
            <div className="card table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Detected</th>
                    <th>Severity</th>
                    <th>Type</th>
                    <th>Asset</th>
                    <th>Status</th>
                    <th>Observed</th>
                    <th>Threshold</th>
                  </tr>
                </thead>
                <tbody>
                  {data.incidents.map((i, n) => (
                    <tr key={i.incident_id ?? n}>
                      <td>{when(i.detected_at)}</td>
                      <td>
                        <span className={`badge ${String(i.severity).toLowerCase()}`}>{i.severity}</span>
                      </td>
                      <td>{i.anomaly_type}</td>
                      <td>{i.asset_id}</td>
                      <td>{i.status}</td>
                      <td>{fmt(i.observed_value, 2)}</td>
                      <td>{fmt(i.threshold, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <h2>
            Readings <a className="link small" href={api.csvUrl(data.meter_id)}>Download CSV</a>
          </h2>
          <div className="card table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Voltage (V)</th>
                  <th>Load (kW)</th>
                  <th>Energy (kWh)</th>
                  <th>Power factor</th>
                </tr>
              </thead>
              <tbody>
                {data.readings.slice(-48).reverse().map((r) => (
                  <tr key={r.t}>
                    <td>{when(r.t)}</td>
                    <td>{fmt(r.voltage)}</td>
                    <td>{fmt(r.kw, 3)}</td>
                    <td>{fmt(r.kwh, 4)}</td>
                    <td>{fmt(r.pf, 3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
