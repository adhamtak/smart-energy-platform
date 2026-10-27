import { useEffect, useState } from 'react'
import { api } from './api.js'
import Login from './Login.jsx'
import Dashboard from './Dashboard.jsx'

export default function App() {
  const [user, setUser] = useState(undefined) // undefined = still checking, null = signed out

  useEffect(() => {
    api.me().then(setUser).catch(() => setUser(null))
  }, [])

  const signOut = async () => {
    try {
      await api.logout()
    } finally {
      setUser(null)
    }
  }

  if (user === undefined) return <div className="center muted">Loading…</div>
  if (user === null) return <Login onLogin={setUser} />
  return <Dashboard user={user} onSignOut={signOut} />
}
