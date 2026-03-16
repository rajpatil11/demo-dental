#!/usr/bin/env python3
"""
Generate the daily EXELVO AI newsletter.

Step 1: Fetch latest AI news from free RSS feeds (no API key needed)
Step 2: Use Gemini free tier to synthesize + format into HTML newsletter

Usage: python tools/generate_newsletter.py
Output: .tmp/newsletter_YYYY-MM-DD.html
Requires: GEMINI_API_KEY in .env or environment (free at aistudio.google.com)
"""

import os
import sys
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import feedparser
from groq import Groq

TODAY = datetime.now()
DATE_DISPLAY = TODAY.strftime("%B %d, %Y")
DATE_FILE = TODAY.strftime("%Y-%m-%d")
DAY_NAME = TODAY.strftime("%A")

# ── RSS feeds (free, no API key) ───────────────────────────────────────────
RSS_FEEDS = [
    ("TechCrunch AI",       "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("VentureBeat AI",      "https://venturebeat.com/category/ai/feed/"),
    ("The Verge AI",        "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"),
    ("Ars Technica",        "https://feeds.arstechnica.com/arstechnica/technology-lab"),
    ("MIT Tech Review",     "https://www.technologyreview.com/feed/"),
    ("Wired AI",            "https://www.wired.com/feed/tag/ai/latest/rss"),
    ("AI News",             "https://www.artificialintelligence-news.com/feed/"),
    ("HuggingFace Blog",    "https://huggingface.co/blog/feed.xml"),
    ("OpenAI Blog",         "https://openai.com/blog/rss.xml"),
    ("Google AI Blog",      "https://blog.google/technology/ai/rss/"),
    ("Anthropic News",      "https://www.anthropic.com/rss.xml"),
    ("Import AI",           "https://importai.substack.com/feed"),
    ("The Batch",           "https://www.deeplearning.ai/the-batch/feed/"),
]


def fetch_news(hours: int = 72) -> list[dict]:
    """Pull articles from RSS feeds published within the last `hours` hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    articles = []

    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
            for entry in feed.entries[:8]:
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub:
                    pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue

                title   = entry.get("title", "").strip()
                summary = re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:400].strip()
                link    = entry.get("link", "")

                if title:
                    articles.append({
                        "source":  source,
                        "title":   title,
                        "summary": summary,
                        "link":    link,
                    })
        except Exception as e:
            print(f"[rss] Warning: could not fetch {source}: {e}")

    print(f"[rss] Fetched {len(articles)} articles from the past {hours}h")
    return articles


# ── CSS ────────────────────────────────────────────────────────────────────
NEWSLETTER_CSS = """
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Helvetica, Arial, sans-serif;
    background: #f0ede8; color: #1a1a1a; -webkit-font-smoothing: antialiased;
  }
  .wrapper { max-width: 640px; margin: 0 auto; background: #ffffff; }
  .header { background: #0c0c0c; padding: 44px 48px 38px; border-bottom: 2px solid #2563eb; }
  .header-eyebrow { font-size: 10px; letter-spacing: 3px; text-transform: uppercase; color: #555; margin-bottom: 16px; }
  .header-brand { font-size: 32px; font-weight: 800; color: #ffffff; letter-spacing: -1.5px; line-height: 1; }
  .header-brand span { color: #3b82f6; }
  .header-meta { display: flex; justify-content: space-between; align-items: center; margin-top: 22px; padding-top: 18px; border-top: 1px solid #1e1e1e; }
  .header-date { color: #666; font-size: 12px; }
  .header-badge { font-size: 9px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; color: #3b82f6; border: 1px solid #1d4ed8; padding: 4px 10px; border-radius: 2px; }
  .body { padding: 0 48px 56px; }
  .section { padding-top: 40px; border-top: 1px solid #ebebeb; }
  .section:first-child { border-top: none; }
  .section-header { display: flex; align-items: baseline; gap: 14px; margin-bottom: 26px; }
  .section-num { font-size: 10px; font-weight: 700; color: #d4cfc8; letter-spacing: 0.5px; flex-shrink: 0; width: 20px; }
  .section-title { font-size: 10px; font-weight: 700; letter-spacing: 3px; text-transform: uppercase; color: #999; }
  .item { margin-bottom: 26px; padding-bottom: 26px; border-bottom: 1px solid #f2f1ee; }
  .item:last-child { border-bottom: none; margin-bottom: 0; padding-bottom: 0; }
  .item-tag { display: inline-flex; align-items: center; gap: 6px; font-size: 10px; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; margin-bottom: 8px; }
  .item-tag::before { content: ''; width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; }
  .tag-breaking { color: #dc2626; } .tag-breaking::before { background: #ef4444; }
  .tag-model    { color: #2563eb; } .tag-model::before    { background: #3b82f6; }
  .tag-tool     { color: #059669; } .tag-tool::before     { background: #10b981; }
  .tag-trend    { color: #d97706; } .tag-trend::before    { background: #f59e0b; }
  .tag-launch   { color: #7c3aed; } .tag-launch::before   { background: #8b5cf6; }
  .tag-voice    { color: #be185d; } .tag-voice::before    { background: #ec4899; }
  .item-title { font-size: 15px; font-weight: 700; color: #0f0f0f; line-height: 1.35; margin-bottom: 9px; letter-spacing: -0.2px; }
  .item-body  { font-size: 13.5px; color: #555; line-height: 1.78; }
  .item-body strong { color: #1a1a1a; font-weight: 600; }
  .stat-grid { display: grid; grid-template-columns: 1fr 1fr; border: 1px solid #ebebeb; border-radius: 8px; overflow: hidden; margin-top: 8px; }
  .stat-card { padding: 22px; border-right: 1px solid #ebebeb; border-bottom: 1px solid #ebebeb; }
  .stat-card:nth-child(2n) { border-right: none; }
  .stat-card:nth-last-child(-n+2) { border-bottom: none; }
  .stat-number { font-size: 28px; font-weight: 800; color: #0f0f0f; letter-spacing: -1.5px; line-height: 1; margin-bottom: 7px; }
  .stat-label  { font-size: 11.5px; color: #888; line-height: 1.55; }
  .insight-box { border-left: 3px solid #0f0f0f; padding: 16px 20px; background: #fafaf8; margin-bottom: 12px; border-radius: 0 6px 6px 0; }
  .insight-box:last-child { margin-bottom: 0; }
  .insight-title { font-size: 13.5px; font-weight: 700; color: #0f0f0f; margin-bottom: 7px; letter-spacing: -0.2px; }
  .insight-body  { font-size: 13px; color: #555; line-height: 1.72; }
  .action-item { display: flex; gap: 18px; padding: 18px 0; border-bottom: 1px solid #f2f1ee; }
  .action-item:last-child { border-bottom: none; }
  .action-num  { font-size: 10px; font-weight: 700; color: #ccc; flex-shrink: 0; width: 20px; padding-top: 3px; }
  .action-text { font-size: 13.5px; color: #444; line-height: 1.72; }
  .action-text strong { color: #0f0f0f; font-weight: 700; display: block; margin-bottom: 4px; font-size: 14px; letter-spacing: -0.1px; }
  .footer { background: #0c0c0c; padding: 30px 48px; }
  .footer-brand   { font-size: 13px; font-weight: 700; color: #fff; margin-bottom: 5px; }
  .footer-meta    { font-size: 11.5px; color: #555; }
  .footer-sources { font-size: 10.5px; color: #3a3a3a; margin-top: 14px; padding-top: 14px; border-top: 1px solid #1a1a1a; line-height: 1.65; }
"""


def build_prompt(articles: list[dict]) -> str:
    news_block = "\n".join(
        f"[{a['source']}] {a['title']}\n{a['summary']}"
        for a in articles
    )

    return f"""Today is {DAY_NAME}, {DATE_DISPLAY}.

You are writing the EXELVO AI Daily Intelligence Briefing for the founder of an AI workflow automation and consulting agency.

Below is today's raw news pulled from AI RSS feeds. Use it as your primary source material.

--- RAW NEWS ---
{news_block}
--- END NEWS ---

Using the news above, generate a complete HTML newsletter with all 8 sections. Synthesize, group, and prioritize — don't just restate headlines. Add context, implications, and founder-relevant insight.

STRICT OUTPUT RULES:
- Output ONLY raw HTML starting with <!DOCTYPE html> — no markdown, no explanation
- Use only real information from the news above (no invented facts)
- Each section needs 2–4 items minimum
- Be concise, specific, and founder-focused
- Available tag classes: tag-breaking · tag-model · tag-tool · tag-trend · tag-launch · tag-voice

HTML TEMPLATE:

<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EXELVO AI Intelligence Briefing — {DATE_DISPLAY}</title>
<style>{NEWSLETTER_CSS}</style>
</head>
<body>
<div class="wrapper">

  <div class="header">
    <div class="header-eyebrow">Daily Intelligence Briefing &nbsp;·&nbsp; Founder Edition</div>
    <div class="header-brand">EXELVO <span>AI</span></div>
    <div class="header-meta">
      <span class="header-date">{DAY_NAME}, {DATE_DISPLAY}</span>
      <span class="header-badge">High Signal</span>
    </div>
  </div>

  <div class="body">

    <!-- SECTION PATTERN:
    <div class="section">
      <div class="section-header">
        <span class="section-num">01</span>
        <span class="section-title">Section Title</span>
      </div>
      <div class="item">
        <div class="item-tag tag-breaking">Tag</div>
        <div class="item-title">Headline</div>
        <div class="item-body">Body with <strong>bold</strong> where needed.</div>
      </div>
    </div> -->

    <!-- STAT GRID (use in sections 03 and 05):
    <div class="stat-grid">
      <div class="stat-card"><div class="stat-number">70%</div><div class="stat-label">label (Source)</div></div>
      <div class="stat-card"><div class="stat-number">10:1</div><div class="stat-label">label (Source)</div></div>
      <div class="stat-card"><div class="stat-number">$11B</div><div class="stat-label">label (Source)</div></div>
      <div class="stat-card"><div class="stat-number">80%</div><div class="stat-label">label (Source)</div></div>
    </div> -->

    <!-- INSIGHT BOXES (section 07):
    <div class="insight-box">
      <div class="insight-title">Title</div>
      <div class="insight-body">Body</div>
    </div> -->

    <!-- ACTION ITEMS (section 08):
    <div class="action-item">
      <div class="action-num">01</div>
      <div class="action-text"><strong>Title.</strong> Explanation.</div>
    </div> -->

    [GENERATE ALL 8 SECTIONS:
     01 Major AI News
     02 LLM Updates
     03 Voice AI & Agent AI  ← end with stat-grid of 4 stats
     04 Automation Tools & SaaS
     05 AI Business & Agency Trends  ← start with stat-grid of 4 stats
     06 Important Launches This Week
     07 Insights for EXELVO AI  ← use insight-boxes (3–4)
     08 Actionable Ideas for EXELVO AI  ← use action-items (5–6)]

  </div>

  <div class="footer">
    <div class="footer-brand">EXELVO AI Daily Intelligence</div>
    <div class="footer-meta">{DATE_DISPLAY} &nbsp;·&nbsp; Workflow Automation &amp; Systems Consulting</div>
    <div class="footer-sources">Sources: {", ".join(sorted(set(a["source"] for a in articles)))}</div>
  </div>

</div>
</body>
</html>"""


def generate_html(articles: list[dict]) -> str:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("Error: GROQ_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    client = Groq(api_key=api_key)
    prompt = build_prompt(articles)

    print("[groq] Generating newsletter HTML...")
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8000,
        temperature=0.4,
    )

    html = response.choices[0].message.content.strip()

    # Strip accidental markdown fences
    if html.startswith("```"):
        html = re.sub(r"^```[a-z]*\n?", "", html)
        html = re.sub(r"\n?```$", "", html).strip()

    # Ensure starts at DOCTYPE
    pos = html.find("<!DOCTYPE html>")
    if pos > 0:
        html = html[pos:]

    if "<!DOCTYPE html>" not in html:
        print("Error: Response does not contain valid HTML", file=sys.stderr)
        print(html[:500], file=sys.stderr)
        sys.exit(1)

    return html.strip()


def main():
    print(f"[generate_newsletter] Generating for {DATE_DISPLAY}...")

    articles = fetch_news(hours=72)
    if not articles:
        print("Warning: No articles fetched — extending window to 96h")
        articles = fetch_news(hours=96)

    html = generate_html(articles)

    out_dir = Path(__file__).parent.parent / ".tmp"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"newsletter_{DATE_FILE}.html"
    out_path.write_text(html, encoding="utf-8")

    print(f"[generate_newsletter] Saved → {out_path}")


if __name__ == "__main__":
    main()
