import { useEffect, useState } from 'react'

// A mechanical-meter style register: each digit is a drum that rolls to its value.
// The last digit (tenths of a kWh) is red, like on a real meter.

function Drum({ digit, index, dim, decimal, ready }) {
  return (
    <span className={`drum${decimal ? ' decimal' : ''}${dim ? ' dim' : ''}`} aria-hidden="true">
      <span
        className="drum-strip"
        style={{ '--d': ready ? digit : 0, transitionDelay: `${index * 70}ms` }}
      >
        {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => (
          <span key={n}>{n}</span>
        ))}
      </span>
    </span>
  )
}

export default function Register({ value, minDigits = 5, label = 'kilowatt-hours' }) {
  const [ready, setReady] = useState(false)

  useEffect(() => {
    setReady(false)
    let second
    const first = requestAnimationFrame(() => {
      second = requestAnimationFrame(() => setReady(true))
    })
    return () => {
      cancelAnimationFrame(first)
      cancelAnimationFrame(second)
    }
  }, [value])

  const safe = value == null || Number.isNaN(Number(value)) ? 0 : Math.max(0, Number(value))
  const [whole, tenth] = safe.toFixed(1).split('.')
  const padded = whole.padStart(minDigits, '0')
  const firstReal = padded.length - whole.length // digits before this index are padding zeros
  const digits = [...padded, ...tenth]

  return (
    <div className="register" role="img" aria-label={value == null ? 'No reading' : `${safe.toFixed(1)} ${label}`}>
      {digits.map((d, i) => (
        <Drum
          key={i}
          digit={Number(d)}
          index={i}
          ready={ready}
          decimal={i === digits.length - 1}
          dim={i < firstReal}
        />
      ))}
    </div>
  )
}
