import { useState } from 'react'

type Props = {
  queries: string[]
}

const SQL_KEYWORDS = /\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP BY|ORDER BY|HAVING|LIMIT|AS|AND|OR|NOT|IN|LIKE|BETWEEN|COUNT|SUM|AVG|MAX|MIN|DISTINCT|WITH|UNION|ALL|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b/g

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

function highlightSql(sql: string): string {
  return esc(sql).replace(SQL_KEYWORDS, '<span class="text-sky-400">$1</span>')
}

function CopySqlButton({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false)

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(sql)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // clipboard API not available
    }
  }

  return (
    <button
      onClick={handleCopy}
      className="absolute top-2 right-2 text-xs text-slate-500 hover:text-slate-300 transition-colors bg-slate-800 px-2 py-0.5 rounded"
      title="Copy SQL"
    >
      {copied ? 'Copied!' : 'Copy'}
    </button>
  )
}

export function SqlPanel({ queries }: Props) {
  if (queries.length === 0) return null

  return (
    <details className="mt-3 text-xs">
      <summary className="cursor-pointer text-slate-400 hover:text-slate-300 select-none">
        {queries.length === 1 ? '1 query' : `${queries.length} queries`} ran
      </summary>
      <div className="mt-2 space-y-2">
        {queries.map((sql, i) => (
          <div key={i} className="relative">
            <pre
              className="bg-slate-900 text-slate-300 rounded p-3 pr-16 overflow-x-auto whitespace-pre-wrap break-words"
              dangerouslySetInnerHTML={{ __html: highlightSql(sql) }}
            />
            <CopySqlButton sql={sql} />
          </div>
        ))}
      </div>
    </details>
  )
}
