export default function Logo({ size = 32 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true">
      <rect width="32" height="32" rx="9" fill="#0F2A5C" />
      <path d="M18 5 9 18h6l-1 9 9-13h-6z" fill="#7DB2FF" />
    </svg>
  )
}
