// Thin wrapper around fetch. The session lives in an httpOnly cookie set by FastAPI.
async function request(path, options = {}) {
  const res = await fetch(path, {
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    let message = res.statusText || 'Request failed'
    try {
      const body = await res.json()
      if (typeof body.detail === 'string') message = body.detail
    } catch {
      /* response was not JSON */
    }
    const error = new Error(message)
    error.status = res.status
    throw error
  }
  return res.json()
}

export const api = {
  login: (username, password) =>
    request('/api/auth/login', { method: 'POST', body: JSON.stringify({ username, password }) }),
  logout: () => request('/api/auth/logout', { method: 'POST' }),
  me: () => request('/api/me'),
  meter: (id) => request(`/api/meters/${encodeURIComponent(id)}`),
  chat: (message, meterId, lang, history) =>
    request('/api/chat', { method: 'POST', body: JSON.stringify({ message, meter_id: meterId, lang, history }) }),
  csvUrl: (id) => `/api/meters/${encodeURIComponent(id)}/readings.csv`,
}
