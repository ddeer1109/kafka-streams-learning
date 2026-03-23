from datetime import datetime


def append_note(filepath, title, content="", tags=None, timestamp=None):
    ts = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M")
    tag_line = f"Tags: {', '.join(f'#{t}' for t in tags)}\n\n" if tags else ""
    # Skip content if it's the same as the title (Google Tasks puts everything in title)
    if content.strip() == title.strip():
        content = ""
    body = f"{content}\n\n" if content else ""
    entry = f"\n## {title}\n\n*{ts}*\n\n{tag_line}{body}---\n"
    with open(filepath, "a") as f:
        f.write(entry)
