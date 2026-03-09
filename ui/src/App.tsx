import { useCallback, useEffect, useRef, useState } from 'react'
import { ChatMessage, type Message } from './components/ChatMessage'

const STARTER_QUESTIONS = [
  "Who scored the most runs in IPL 2023?",
  "Which teams played the most matches in 2022?",
  "What is Virat Kohli's average in T20Is?",
  "Top 5 ODI wicket-takers since 2010?",
]

function App() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const userScrolledUp = useRef(false)

  // Auto-scroll: only scroll to bottom if user hasn't scrolled up manually
  useEffect(() => {
    if (!userScrolledUp.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages, loading])

  const handleScroll = useCallback(() => {
    const el = scrollContainerRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
    userScrolledUp.current = !atBottom
  }, [])

  function handleNewConversation() {
    setMessages([])
    setInput('')
    setError(null)
    userScrolledUp.current = false
  }

  async function submitQuestion(question: string) {
    if (!question.trim() || loading) return

    const userMessage: Message = { role: 'user', content: question }
    const updatedMessages = [...messages, userMessage]
    setMessages(updatedMessages)
    setInput('')
    setLoading(true)
    setError(null)
    userScrolledUp.current = false

    // Build history for the API: only text-based turns (strip queries metadata)
    const history = updatedMessages.slice(0, -1).map((m) => ({
      role: m.role,
      content: m.content,
    }))

    try {
      const res = await fetch('/api/ask/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, history }),
      })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        throw new Error(data.detail ?? `Server error ${res.status}`)
      }

      const reader = res.body?.getReader()
      if (!reader) throw new Error('No response body')

      const decoder = new TextDecoder()
      let buffer = ''
      let streamedContent = ''
      const queries: string[] = []

      // Add a placeholder assistant message for streaming
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: '', queries: [] },
      ])

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const jsonStr = line.slice(6).trim()
          if (!jsonStr) continue

          try {
            const event = JSON.parse(jsonStr)
            if (event.type === 'token') {
              streamedContent += event.content
              setMessages((prev) => {
                const updated = [...prev]
                const last = updated[updated.length - 1]
                if (last?.role === 'assistant') {
                  updated[updated.length - 1] = {
                    ...last,
                    content: streamedContent,
                    queries: [...queries],
                  }
                }
                return updated
              })
            } else if (event.type === 'query') {
              queries.push(event.sql)
            } else if (event.type === 'done') {
              // Final update with all queries
              setMessages((prev) => {
                const updated = [...prev]
                const last = updated[updated.length - 1]
                if (last?.role === 'assistant') {
                  updated[updated.length - 1] = {
                    ...last,
                    content: streamedContent,
                    queries: event.queries ?? queries,
                  }
                }
                return updated
              })
            } else if (event.type === 'error') {
              throw new Error(event.detail)
            }
          } catch (parseErr) {
            // Skip malformed SSE lines
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    submitQuestion(input.trim())
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      submitQuestion(input.trim())
    }
  }

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-100">
      {/* Header */}
      <header className="border-b border-slate-800 px-6 py-4 flex items-center gap-3">
        <span className="text-2xl">🏏</span>
        <div className="flex-1">
          <h1 className="font-semibold text-lg leading-none">Cricket Agent</h1>
          <p className="text-xs text-slate-400 mt-0.5">Ask anything about ODI & T20I cricket</p>
        </div>
        {messages.length > 0 && (
          <button
            onClick={handleNewConversation}
            className="text-xs text-slate-400 hover:text-slate-200 border border-slate-700 hover:border-slate-500 rounded-lg px-3 py-1.5 transition-colors"
          >
            New conversation
          </button>
        )}
      </header>

      {/* Messages */}
      <div
        ref={scrollContainerRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto px-4 py-6 space-y-4"
      >
        {messages.length === 0 && (
          <div className="text-center mt-20">
            <div className="text-6xl mb-4">🏏</div>
            <h2 className="text-xl font-bold text-slate-200 mb-1">Cricket Agent</h2>
            <p className="text-sm text-slate-500">Powered by Claude + live Cricsheet data</p>
            <div className="flex flex-wrap justify-center gap-2 mt-6 max-w-2xl mx-auto">
              {STARTER_QUESTIONS.map((q) => (
                <button
                  key={q}
                  onClick={() => submitQuestion(q)}
                  className="text-sm bg-slate-800/80 hover:bg-slate-700 text-slate-300 hover:text-slate-100 border-l-2 border-indigo-500 rounded-lg px-4 py-2.5 transition-all text-left"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg, i) => (
          <ChatMessage key={i} message={msg} isStreaming={loading && i === messages.length - 1} />
        ))}
        {loading && messages[messages.length - 1]?.role !== 'assistant' && (
          <div className="flex justify-start">
            <div className="bg-slate-800 rounded-2xl rounded-bl-sm px-4 py-3">
              <div className="flex gap-1 items-center">
                <span className="w-2 h-2 rounded-full bg-slate-500 animate-bounce [animation-delay:-0.3s]" />
                <span className="w-2 h-2 rounded-full bg-slate-500 animate-bounce [animation-delay:-0.15s]" />
                <span className="w-2 h-2 rounded-full bg-slate-500 animate-bounce" />
              </div>
            </div>
          </div>
        )}
        {error && (
          <div className="flex justify-center">
            <div className="bg-red-900/50 text-red-300 rounded-lg px-4 py-2 text-sm">
              Error: {error}
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <form
        onSubmit={handleSubmit}
        className="border-t border-slate-800 px-4 py-4 flex gap-3"
      >
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a cricket question... (Cmd+Enter to send)"
          disabled={loading}
          rows={1}
          className="flex-1 bg-slate-800 rounded-xl px-4 py-3 text-sm placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-50 resize-none"
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 disabled:cursor-not-allowed rounded-xl px-5 py-3 text-sm font-medium transition-colors"
        >
          Send
        </button>
      </form>
    </div>
  )
}

export default App
