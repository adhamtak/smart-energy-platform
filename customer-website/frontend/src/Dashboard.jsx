import { useEffect, useState } from 'react'
import { api } from './api.js'
import { LangToggle, useI18n } from './i18n.jsx'
import Chat from './Chat.jsx'
import Logo from './Logo.jsx'
import Register from './Register.jsx'
import { Panel, consumptionChart, powerFactorChart, voltageChart } from './ChartCard.jsx'

const fmt = (v, digits = 1) => (v == null ? '–' : Number(v).toLocaleString('en-US', { maximumFractionDigits: digits }))
const when = (v) => (v ? String(v).replace('T', ' ').slice(0, 16) : '–')

// Small "?" that explains a technical term in plain words (hover, tap or keyboard focus).
function Hint({ text }) {
  return <span className="hint" tabIndex={0} role="img" aria-label={text} data-tip={text}>?</span>
}

function Stat({ label, hint, value, unit, sub }) {
  return (
    <div className="stat">
      <div className="stat-label">
        {label}
        {hint && <Hint text={hint} />}
      </div>
      <div className="stat-value ltr">
        {value}
        {unit && <span className="unit"> {unit}</span>}
      </div>
      {sub && <div className="stat-sub ltr">{sub}</div>}
    </div>
  )
}

function Fact({ label, hint, value }) {
  if (!value) return null
  return (
    <div className="fact">
      <dt>
        {label}
        {hint && <Hint text={hint} />}
      </dt>
      <dd>{value}</dd>
    </div>
  )
}

function Skeleton({ label }) {
  return (
    <div className="skeleton" role="status" aria-label={label}>
      <div className="sk sk-hero" />
      <div className="sk sk-side" />
      <div className="sk sk-strip" />
      <div className="sk sk-chart" />
    </div>
  )
}

export default function Dashboard({ user, onSignOut }) {
  const { t, tv } = useI18n()
  const defaultMeter = user.is_admin ? 'MTR000001' : user.meters[0]
  const [input, setInput] = useState(defaultMeter)
  const [meterId, setMeterId] = useState(defaultMeter)
  const [data, setData] = useState(null)
  const [error, setError] = useState(null) // { kind, id, message }
  const [attempt, setAttempt] = useState(0)
  const [loading, setLoading] = useState(false)
  const [view, setView] = useState('hour')

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api
      .meter(meterId)
      .then((d) => {
        if (cancelled) return
        setData(d)
        setView(d.daily.length > 0 ? 'day' : 'hour')
      })
      .catch((e) => {
        if (cancelled) return
        if (e.status === 401) return onSignOut() // session expired
        setData(null)
        if (e.status === 400 || e.status === 404) setError({ kind: 'notFound', id: meterId })
        else if (e.status === 403) setError({ kind: 'noAccess', id: meterId })
        else setError({ kind: 'generic', id: meterId, message: e.message })
      })
      .finally(() => !cancelled && setLoading(false))
    return () => {
      cancelled = true
    }
  }, [meterId, attempt]) // eslint-disable-line react-hooks/exhaustive-deps

  const submit = (event) => {
    event.preventDefault()
    const id = input.trim().toUpperCase()
    if (!id) return
    setMeterId(id)
    setAttempt((n) => n + 1)
  }

  const s = data?.summary
  const multiDay = s?.multi_day ?? false
  const hasDaily = (data?.daily.length ?? 0) > 0
  const lowShare = s && s.n_readings ? Math.min(100, (s.low_voltage_intervals / s.n_readings) * 100) : 0
  const voltageOk = s && s.low_voltage_intervals === 0
  const first = data?.readings[0]?.t
  const last = data?.readings[data.readings.length - 1]?.t

  const views = [
    { id: 'hour', label: t('tab.hour'), note: t('note.hour') },
    ...(hasDaily ? [{ id: 'day', label: t('tab.day'), note: t('note.day') }] : []),
    { id: 'load', label: t('tab.load'), note: t('note.load') },
  ]
  const current = views.find((v) => v.id === view) ?? views[0]

  // Plain-language summary, built only from numbers that are in the data.
  const glance = []
  if (s) {
    glance.push({
      tone: 'info',
      text: s.avg_daily_kwh != null
        ? t('sum.usage', { kwh: fmt(s.total_kwh), avg: fmt(s.avg_daily_kwh) })
        : t('sum.usageShort', { kwh: fmt(s.total_kwh) }),
    })
    if (s.peak_kw != null) glance.push({ tone: 'info', text: t('sum.peak', { kw: fmt(s.peak_kw, 2), time: when(s.peak_time) }) })
    glance.push(
      voltageOk
        ? { tone: 'ok', text: t('sum.voltageOk', { min: fmt(s.min_voltage) }) }
        : { tone: 'bad', text: t('sum.voltageLow', { limit: data.low_voltage_threshold, n: s.low_voltage_intervals, total: s.n_readings }) },
    )
    glance.push(
      data.incidents.length > 0
        ? { tone: 'warn', text: t('sum.incidents', { n: data.incidents.length }) }
        : { tone: 'ok', text: t('sum.noIncidents') },
    )
  }

  const errorTitle = error && (error.kind === 'notFound' ? t('err.notFoundTitle') : error.kind === 'noAccess' ? t('err.noAccessTitle') : t('err.genericTitle'))
  const errorText = error && (error.kind === 'notFound' ? t('err.notFound', { id: error.id }) : error.kind === 'noAccess' ? t('err.noAccess', { id: error.id }) : error.message)

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand dark">
          <Logo />
          <span>{t('app.name')}</span>
        </div>
        <div className="topbar-right">
          <span className="user-chip">
            {user.username}
            {user.is_admin && <em>{t('top.admin')}</em>}
          </span>
          <LangToggle />
          <button className="ghost" onClick={onSignOut}>
            {t('top.signOut')}
          </button>
        </div>
      </header>

      <main className="page">
        <form className="meter-bar" onSubmit={submit}>
          <label htmlFor="meter-input">{t('meter.label')}</label>
          {user.is_admin ? (
            <input id="meter-input" dir="ltr" value={input} onChange={(e) => setInput(e.target.value)} placeholder="MTR000001" spellCheck="false" />
          ) : (
            <select id="meter-input" dir="ltr" value={input} onChange={(e) => setInput(e.target.value)}>
              {user.meters.map((m) => (
                <option key={m}>{m}</option>
              ))}
            </select>
          )}
          <button className="primary" disabled={loading}>
            {loading ? t('common.loading') : t('meter.show')}
          </button>
        </form>

        {error && (
          <div className="notice" role="alert">
            <h2>{errorTitle}</h2>
            <p>{errorText}</p>
          </div>
        )}

        {loading && !data && <Skeleton label={t('common.loading')} />}

        {data && (
          <div className={`content${loading ? ' is-loading' : ''}`}>
            <div className="meter-head">
              <h1 className="ltr">{data.meter_id}</h1>
              <dl className="facts">
                <Fact label={t('fact.customer')} value={tv('cust', data.info.customer_type)} />
                <Fact label={t('fact.phase')} value={tv('ph', data.info.phase_type)} />
                <Fact label={t('fact.transformer')} hint={t('hint.transformer')} value={data.info.transformer_id && <span className="ltr">{data.info.transformer_id}</span>} />
                <Fact label={t('fact.status')} value={tv('ms', data.info.status)} />
              </dl>
            </div>

            <section className="glance" aria-labelledby="glance-title">
              <h2 id="glance-title">{t('sum.title')}</h2>
              <ul>
                {glance.map((g, i) => (
                  <li key={i} className={g.tone}>
                    <span className="dot" aria-hidden="true" />
                    <span>{g.text}</span>
                  </li>
                ))}
              </ul>
            </section>

            <div className="hero-grid">
              <section className="hero">
                <div className="hero-label">{t('hero.label')}</div>
                <div className="hero-reg">
                  <Register value={s.total_kwh} label={t('unit.kwhLong')} />
                  <span className="hero-unit">kWh</span>
                </div>
                <div className="hero-foot">
                  {first && last ? t('hero.range', { from: when(first), to: when(last) }) : t('hero.noReadings')}
                  <span> · {t('hero.meta', { n: fmt(s.n_readings, 0), tz: data.timezone })}</span>
                </div>
              </section>

              <section className={`voltage-card ${voltageOk ? 'ok' : 'warn'}`}>
                <div className="vc-top">
                  <span className="vc-label">
                    {t('volt.label')}
                    <Hint text={t('hint.voltage')} />
                  </span>
                  <span className="vc-status">{voltageOk ? t('volt.stable') : t('volt.dipped')}</span>
                </div>
                <div>
                  <div className="stat-value ltr">
                    {fmt(s.min_voltage)}
                    <span className="unit"> V</span>
                  </div>
                  <div className="stat-sub">{t('volt.lowest')}</div>
                </div>
                <div className="bar" role="img" aria-label={`${fmt(lowShare, 1)}%`}>
                  <i style={{ width: `${lowShare}%` }} />
                </div>
                <p className="vc-text">
                  {voltageOk
                    ? t('volt.ok', { limit: data.low_voltage_threshold, avg: fmt(s.avg_voltage) })
                    : t('volt.low', { limit: data.low_voltage_threshold, n: s.low_voltage_intervals, total: s.n_readings, avg: fmt(s.avg_voltage) })}
                </p>
              </section>
            </div>

            <section className="stat-strip">
              <Stat label={t('stat.avgDay')} hint={t('hint.avgDay')} value={fmt(s.avg_daily_kwh)} unit="kWh" />
              <Stat label={t('stat.peak')} hint={t('hint.peak')} value={fmt(s.peak_kw, 2)} unit="kW" sub={when(s.peak_time)} />
              <Stat label={t('stat.pf')} hint={t('hint.pf')} value={fmt(s.avg_power_factor, 2)} />
            </section>

            <Panel
              title={t('chart.consumption')}
              note={current.note}
              action={
                <div className="tabs" role="tablist" aria-label={t('chart.view')}>
                  {views.map((v) => (
                    <button key={v.id} role="tab" aria-selected={view === v.id} className={view === v.id ? 'on' : ''} onClick={() => setView(v.id)}>
                      {v.label}
                    </button>
                  ))}
                </div>
              }
            >
              {consumptionChart({ view: current.id, hourly: data.hourly, daily: data.daily, readings: data.readings, multiDay })}
            </Panel>

            <div className="two-up">
              <Panel title={t('chart.voltage')} note={t('chart.voltageNote', { limit: data.low_voltage_threshold })}>
                {voltageChart({ readings: data.readings, threshold: data.low_voltage_threshold, multiDay, limitLabel: t('chart.limit', { limit: data.low_voltage_threshold }) })}
              </Panel>
              <Panel title={t('chart.pf')} note={t('chart.pfNote')}>
                {powerFactorChart({ readings: data.readings, multiDay })}
              </Panel>
            </div>

            <section className="block">
              <div className="block-head">
                <h2>{t('inc.title')}</h2>
                {data.incidents.length > 0 && <span className="count">{data.incidents.length}</span>}
              </div>
              {data.incidents.length > 0 ? (
                <div className="table-card">
                  <table>
                    <thead>
                      <tr>
                        <th>{t('inc.detected')}</th>
                        <th>{t('inc.severity')}</th>
                        <th>{t('inc.type')}</th>
                        <th>{t('inc.asset')}</th>
                        <th>{t('inc.status')}</th>
                        <th className="num">{t('inc.observed')}</th>
                        <th className="num">{t('inc.threshold')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.incidents.map((i, n) => (
                        <tr key={i.incident_id ?? n}>
                          <td className="ltr-cell">{when(i.detected_at)}</td>
                          <td>
                            <span className={`badge ${String(i.severity).toLowerCase()}`}>{tv('sev', i.severity)}</span>
                          </td>
                          <td>{tv('an', i.anomaly_type)}</td>
                          <td className="ltr-cell">{i.asset_id}</td>
                          <td>{tv('st', i.status)}</td>
                          <td className="num">{fmt(i.observed_value, 2)}</td>
                          <td className="num">{fmt(i.threshold, 2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="empty">{t('inc.empty')}</p>
              )}
            </section>

            <section className="block">
              <div className="block-head">
                <h2>{t('read.title')}</h2>
                <a className="download" href={api.csvUrl(data.meter_id)}>
                  {t('read.csv')}
                </a>
              </div>
              <div className="table-card scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t('read.time')}</th>
                      <th className="num">{t('read.voltage')}</th>
                      <th className="num">{t('read.load')}</th>
                      <th className="num">{t('read.energy')}</th>
                      <th className="num">{t('read.pf')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.readings
                      .slice(-48)
                      .reverse()
                      .map((r) => (
                        <tr key={r.t}>
                          <td className="ltr-cell">{when(r.t)}</td>
                          <td className="num">{fmt(r.voltage)}</td>
                          <td className="num">{fmt(r.kw, 3)}</td>
                          <td className="num">{fmt(r.kwh, 4)}</td>
                          <td className="num">{fmt(r.pf, 3)}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        )}
      </main>

      <Chat meterId={data?.meter_id ?? meterId} />
    </div>
  )
}
