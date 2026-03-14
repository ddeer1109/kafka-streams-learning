# LLM Context Window Management

tags: llm, prompt, context, model

Strategies for working within context limits:

1. **Chunking** — Split large documents into overlapping chunks (512-1024 tokens, 10-20% overlap)
2. **Summarization chains** — Summarize sections, then summarize summaries
3. **Selective retrieval** — Use embeddings to find only the relevant chunks
4. **System prompt economy** — Keep system prompts concise; move examples to retrieval

Token estimation rules of thumb:
- English: ~4 characters per token
- Code: ~3 characters per token (more special chars)
- JSON: ~2.5 characters per token (lots of punctuation)
