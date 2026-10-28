import { useState } from 'react'
import { api } from './api.js'
import { LangToggle, useI18n } from './i18n.jsx'
import Logo from './Logo.jsx'
import Register from './Register.jsx'

export default function Login({ onLogin }) {
  const { t } = useI18n()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  const submit = async (event) => {
    event.preventDefault()
    setBusy(true)
    setError('')
    try {
      onLogin(await api.login(username, password))
    } catch (e) {
      const known = t(`err.${e.message}`)
      setError(known === `err.${e.message}` ? e.message : known)
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="login-page">
      <aside className="login-hero">
        <div className="brand">
          <Logo size={34} />
          <span>{t('app.name')}</span>
        </div>
        <div className="login-hero-body">
          <Register value={48213.7} label={t('unit.kwhLong')} />
          <h2>{t('login.heroTitle')}</h2>
          <p>{t('login.heroText')}</p>
        </div>
        <p className="login-foot">{t('login.demo')}</p>
      </aside>

      <section className="login-side">
        <div className="login-lang">
          <LangToggle />
        </div>
        <form className="login-form" onSubmit={submit}>
          <h1>{t('login.title')}</h1>
          <p className="muted">{t('login.sub')}</p>
          <label>
            {t('login.username')}
            <input dir="ltr" value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" autoFocus required />
          </label>
          <label>
            {t('login.password')}
            <input dir="ltr" type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required />
          </label>
          {error && (
            <div className="alert error" role="alert">
              {error}
            </div>
          )}
          <button className="primary" disabled={busy}>
            {busy ? t('login.busy') : t('login.submit')}
          </button>
        </form>
      </section>
    </main>
  )
}
