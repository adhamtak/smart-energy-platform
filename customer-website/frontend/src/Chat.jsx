import { useEffect, useRef, useState } from 'react'
import { api } from './api.js'
import { useI18n } from './i18n.jsx'

export default function Chat({ meterId }) {
  const { t, lang } = useI18n()
  const [open, setOpen] = useState(false)
  const [messages, setMessages] = useState([]) // { role, content, engine?, error? }
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const endRef = useRef(null)
  const inputRef = useRef(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: 'end' })
  }, [messages, busy, open])

  useEffect(() => {
    if (open) inputRef.current?.focus()
  }, [open])

  const send = async (raw) => {
    const message = (raw ?? text).trim()
    if (!message || busy) return
    const history = messages.filter((m) => !m.error).slice(-6).map(({ role, content }) => ({ role, content }))
    setMessages((m) => [...m, { role: 'user', content: message }])
    setText('')
    setBusy(true)
    try {
      const res = await api.chat(message, meterId, lang, history)
      setMessages((m) => [...m, { role: 'assistant', content: res.reply, engine: res.engine }])
    } catch (e) {
      if (e.status === 401) return window.location.reload() // session expired
      setMessages((m) => [...m, { role: 'assistant', content: t('chat.error'), error: true }])
    } finally {
      setBusy(false)
    }
  }

  const submit = (event) => {
    event.preventDefault()
    send()
  }

  if (!open) {
    return (
      <button className="chat-launcher" onClick={() => setOpen(true)} aria-label={t('chat.open')}>
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.6A8 8 0 1 1 21 12z" />
        </svg>
        <span>{t('chat.open')}</span>
      </button>
    )
  }

  return (
    <section className="chat" aria-label={t('chat.title')}>
      <header className="chat-head">
        <div>
          <strong>{t('chat.title')}</strong>
          <span className="chat-sub ltr">{meterId}</span>
        </div>
        <button className="chat-close" onClick={() => setOpen(false)} aria-label={t('chat.close')}>
          ×
        </button>
      </header>

      <div className="chat-body" aria-live="polite">
        {messages.length === 0 && (
          <div className="chat-intro">
            <p>{t('chat.hello')}</p>
            <div className="chips">
              {['chat.s1', 'chat.s2', 'chat.s3', 'chat.s4'].map((k) => (
                <button key={k} type="button" onClick={() => send(t(k))}>
                  {t(k)}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`bubble ${m.role}${m.error ? ' err' : ''}`}>
            <div className="bubble-text">{m.content}</div>
            {m.engine === 'rules' && <div className="bubble-meta">{t('chat.basic')}</div>}
          </div>
        ))}

        {busy && <div className="bubble assistant typing">{t('chat.thinking')}</div>}
        <div ref={endRef} />
      </div>

      <form className="chat-form" onSubmit={submit}>
        <input ref={inputRef} value={text} onChange={(e) => setText(e.target.value)} placeholder={t('chat.placeholder')} maxLength={500} />
        <button className="primary" disabled={busy || !text.trim()}>
          {t('chat.send')}
        </button>
      </form>
      <p className="chat-note">{t('chat.note')}</p>
    </section>
  )
}
