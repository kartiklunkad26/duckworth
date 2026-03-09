import { useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { SqlPanel } from './SqlPanel'

export type Message = {
  role: 'user' | 'assistant'
  content: string
  queries?: string[]
}

type Props = {
  message: Message
  isStreaming?: boolean
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // clipboard API not available
    }
  }

  return (
    <button
      onClick={handleCopy}
      className="text-xs text-slate-500 hover:text-slate-300 transition-colors"
      title="Copy to clipboard"
    >
      {copied ? 'Copied!' : 'Copy'}
    </button>
  )
}

export function ChatMessage({ message, isStreaming }: Props) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-3xl rounded-2xl px-4 py-3 ${
          isUser
            ? 'bg-indigo-600 text-white rounded-br-sm'
            : 'bg-slate-800 text-slate-100 rounded-bl-sm'
        }`}
      >
        {isUser ? (
          <p className="text-sm">{message.content}</p>
        ) : (
          <div className="prose prose-sm prose-invert max-w-none">
            <div className="flex justify-end mb-1">
              <CopyButton text={message.content} />
            </div>
            <ReactMarkdown>{message.content}</ReactMarkdown>
            {isStreaming && <span className="inline-block w-0.5 h-4 bg-slate-300 animate-pulse ml-0.5 align-text-bottom" />}
            <SqlPanel queries={message.queries ?? []} />
          </div>
        )}
      </div>
    </div>
  )
}
