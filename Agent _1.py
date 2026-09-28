"""
Agent 1: Content Discovery & Packaging Agent
=============================================

Pipeline:
    1. Integration     -> fetch_rss_feeds(), fetch_news_api()
    2. Classification   -> classify_content()
    3. Generation        -> generate_text_script(), generate_image_prompt(), generate_video_script()
    4. Packaging + Email -> build_package(), send_email()

Two ways to run:

  A) SINGLE-PASS PIPELINE (original behavior)
     Call run_pipeline() / main() once (manually, via cron, via a cloud function
     trigger, or via a workflow orchestrator like Airflow/Step Functions) to pull
     from RSS/News sources and email a generated content package.

  B) INBOX-COMMAND MODE (new)
     Call check_inbox_for_commands() once to poll an IMAP inbox for unread task
     emails (e.g. from a Director/orchestrator agent, or a human), parse them into
     a structured, whitelisted command, generate content for the requested topic
     using the SAME generator functions as the pipeline, and email a reply back.

     This does NOT execute arbitrary code/shell instructions found in email bodies.
     Only a fixed set of recognized fields (topic, content type, constraints) are
     read from allow-listed senders, and are only ever used as data passed into
     generate_text_script/generate_image_prompt/generate_video_script — never
     eval'd, exec'd, or passed to a shell. See COMMAND FORMAT below.

Neither mode loops or self-schedules. For recurring execution, call the relevant
entry point from cron, a scheduler, or a small external loop with a sleep.

Dependencies:
    pip install feedparser requests

Configuration:
    Fill in the CONFIG dict below or set the corresponding environment variables.

COMMAND FORMAT (inbox-command mode)
------------------------------------
Agent 1 only acts on emails that satisfy ALL of:
  1. Sender address is in CONFIG["ALLOWED_COMMAND_SENDERS"].
  2. Subject line contains the marker CONFIG["COMMAND_SUBJECT_MARKER"]
     (default: "Discovery Request"), matching the convention used by the
     Lead Director orchestrator's dispatch_to_discovery().
  3. Body contains recognizable "Key: value" lines, specifically:
        Run ID: <id>              (optional; echoed back in the reply subject)
        Topic: <free text>        (required; used as the ContentItem title/summary)
        Constraints: <JSON dict>  (optional; e.g. {"platform": "tiktok"})
        Content-Type: <one of text_script|image_prompt|video_script>  (optional;
                                   if omitted, classify_content() decides)

Any other text in the email body is ignored — it is never executed, only the
specific labeled fields above are extracted (via regex) and only ever used as
plain data (strings/dict) fed into the existing generator functions.
"""

import os
import re
import ssl
import json
import time
import smtplib
import imaplib
import logging
import email as email_lib
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Optional, Tuple

import requests
import feedparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("agent1")


# ---------------------------------------------------------------------------
# CONFIG — fill these in, or set as environment variables of the same name.
# ---------------------------------------------------------------------------
CONFIG = {
    # News API (e.g. newsapi.org) — placeholder key
    "NEWS_API_KEY": os.getenv("NEWS_API_KEY", "YOUR_NEWS_API_KEY_HERE"),
    "NEWS_API_URL": "https://newsapi.org/v2/top-headlines",
    "NEWS_API_PARAMS": {"category": "technology", "language": "en", "pageSize": 20},

    # RSS feeds to poll — add/remove as needed
    "RSS_FEEDS": [
        "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "https://www.theverge.com/rss/index.xml",
    ],

    # SMTP (outgoing) settings — placeholders
    "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", "465")),
    "SMTP_USERNAME": os.getenv("SMTP_USERNAME", "YOUR_EMAIL@example.com"),
    "SMTP_PASSWORD": os.getenv("SMTP_PASSWORD", "YOUR_APP_PASSWORD_HERE"),
    "EMAIL_FROM": os.getenv("EMAIL_FROM", "YOUR_EMAIL@example.com"),
    "EMAIL_TO": os.getenv("EMAIL_TO", "video-editor@example.com"),

    # IMAP (incoming / inbox-command mode) settings — placeholders
    "IMAP_HOST": os.getenv("IMAP_HOST", "imap.gmail.com"),
    "IMAP_PORT": int(os.getenv("IMAP_PORT", "993")),
    "IMAP_USERNAME": os.getenv("IMAP_USERNAME", os.getenv("SMTP_USERNAME", "YOUR_EMAIL@example.com")),
    "IMAP_PASSWORD": os.getenv("IMAP_PASSWORD", os.getenv("SMTP_PASSWORD", "YOUR_APP_PASSWORD_HERE")),
    "IMAP_MAILBOX": os.getenv("IMAP_MAILBOX", "INBOX"),

    # Security: only these sender addresses are allowed to trigger inbox commands.
    # Comma-separated in the env var, e.g. "director@example.com,me@example.com"
    "ALLOWED_COMMAND_SENDERS": [
        addr.strip().lower()
        for addr in os.getenv("ALLOWED_COMMAND_SENDERS", "").split(",")
        if addr.strip()
    ],

    # Only emails whose subject contains this marker are treated as commands.
    "COMMAND_SUBJECT_MARKER": os.getenv("COMMAND_SUBJECT_MARKER", "Discovery Request"),

    # Max commands processed per check_inbox_for_commands() call.
    "MAX_COMMANDS_PER_CHECK": int(os.getenv("MAX_COMMANDS_PER_CHECK", "5")),

    # How many top items to process per pipeline run
    "MAX_ITEMS_PER_RUN": 5,
}


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------
@dataclass
class ContentItem:
    title: str
    summary: str
    url: str = ""
    source: str = ""
    content_type: str = ""          # "text_script" | "image_prompt" | "video_script"
    generated_output: Dict = field(default_factory=dict)


@dataclass
class InboxCommand:
    """A structured, whitelisted representation of a command email.
    Only the fields below are ever extracted from the email — nothing else
    in the message body is read or acted on."""
    run_id: str = ""
    topic: str = ""
    constraints: Dict = field(default_factory=dict)
    content_type: str = ""          # "" means "let classify_content() decide"
    from_address: str = ""
    subject: str = ""


# ---------------------------------------------------------------------------
# 1. INTEGRATION — data fetching
# ---------------------------------------------------------------------------
def fetch_rss_feeds(feed_urls: Optional[List[str]] = None) -> List[ContentItem]:
    """
    Pull entries from a list of RSS feed URLs using feedparser.
    Returns a list of ContentItem objects (unclassified).
    """
    feed_urls = feed_urls or CONFIG["RSS_FEEDS"]
    items: List[ContentItem] = []

    for url in feed_urls:
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries:
                items.append(
                    ContentItem(
                        title=entry.get("title", "").strip(),
                        summary=re.sub("<[^<]+?>", "", entry.get("summary", "")).strip(),
                        url=entry.get("link", ""),
                        source=parsed.feed.get("title", url),
                    )
                )
        except Exception as exc:
            logger.warning("Failed to parse RSS feed %s: %s", url, exc)

    logger.info("Fetched %d items from %d RSS feeds", len(items), len(feed_urls))
    return items


def fetch_news_api() -> List[ContentItem]:
    """
    Query a news API (e.g. NewsAPI.org) for top headlines.
    Requires CONFIG['NEWS_API_KEY'] to be set to a real key.
    Returns a list of ContentItem objects (unclassified).
    """
    api_key = CONFIG["NEWS_API_KEY"]
    if not api_key or api_key.startswith("YOUR_"):
        logger.warning("NEWS_API_KEY is not configured — skipping news API fetch.")
        return []

    params = dict(CONFIG["NEWS_API_PARAMS"])
    params["apiKey"] = api_key

    try:
        resp = requests.get(CONFIG["NEWS_API_URL"], params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("News API request failed: %s", exc)
        return []

    items: List[ContentItem] = []
    for article in data.get("articles", []):
        items.append(
            ContentItem(
                title=(article.get("title") or "").strip(),
                summary=(article.get("description") or "").strip(),
                url=article.get("url", ""),
                source=(article.get("source") or {}).get("name", "NewsAPI"),
            )
        )

    logger.info("Fetched %d items from News API", len(items))
    return items


def fetch_social_placeholder(platform: str = "generic") -> List[ContentItem]:
    """
    Placeholder for a social media scraping integration (e.g. X/Twitter API,
    TikTok trending API, Reddit API). Implement using that platform's official
    API and your own credentials — do not scrape in violation of a platform's
    terms of service.
    """
    logger.info("fetch_social_placeholder(%s) called — no integration implemented.", platform)
    return []


# ---------------------------------------------------------------------------
# 2. CLASSIFICATION — decide the best content format
# ---------------------------------------------------------------------------
# Simple keyword-based heuristics. Swap this out for an LLM-based classifier
# or a trained model if you want more nuanced decisions.
VIDEO_KEYWORDS = {
    "watch", "video", "footage", "viral", "clip", "trend", "dance",
    "challenge", "reaction", "livestream", "stream",
}
IMAGE_KEYWORDS = {
    "photo", "image", "art", "design", "aesthetic", "visual", "poster",
    "illustration", "render", "concept art",
}
TEXT_KEYWORDS = {
    "report", "analysis", "study", "policy", "research", "announcement",
    "earnings", "statement", "interview", "opinion",
}


def classify_content(item: ContentItem) -> str:
    """
    Classify a ContentItem into one of:
        "video_script"  -> best told as a short-form video
        "image_prompt"  -> best told as a single generated image
        "text_script"   -> best told as a written article / long-form script

    Rule-based scoring: count keyword hits per category in title+summary,
    then fall back to length-based heuristics if no keywords match.
    """
    text = f"{item.title} {item.summary}".lower()

    video_score = sum(1 for kw in VIDEO_KEYWORDS if kw in text)
    image_score = sum(1 for kw in IMAGE_KEYWORDS if kw in text)
    text_score = sum(1 for kw in TEXT_KEYWORDS if kw in text)

    scores = {
        "video_script": video_score,
        "image_prompt": image_score,
        "text_script": text_score,
    }
    best_type = max(scores, key=scores.get)

    # Fallback heuristic if nothing matched any keyword list
    if scores[best_type] == 0:
        if len(item.summary) < 100:
            best_type = "video_script"   # short, punchy topics -> short-form video
        else:
            best_type = "text_script"    # longer topics -> written script/article

    item.content_type = best_type
    logger.info("Classified '%s' as %s (scores=%s)", item.title[:50], best_type, scores)
    return best_type


# ---------------------------------------------------------------------------
# 3. CONTENT GENERATION — format output per content type
# ---------------------------------------------------------------------------
def generate_text_script(item: ContentItem) -> Dict:
    """
    Produce a structured long-form text script/article outline.
    NOTE: This builds a template from the source data. Plug in an LLM call
    here (e.g. the Anthropic API) to have the body paragraphs auto-written.
    """
    return {
        "type": "text_script",
        "headline": item.title,
        "hook": f"Did you know: {item.title}?",
        "outline": [
            "Intro — hook + why this matters now",
            f"Background: {item.summary}",
            "Key details / data points",
            "Implications for the audience",
            "Closing takeaway + call to action",
        ],
        "source_url": item.url,
    }


def generate_image_prompt(item: ContentItem) -> Dict:
    """
    Produce a structured prompt suitable for an image-generation model
    (e.g. Midjourney, DALL-E, Stable Diffusion).
    """
    return {
        "type": "image_prompt",
        "headline": item.title,
        "prompt": (
            f"A striking, high-detail editorial illustration representing: {item.title}. "
            f"Context: {item.summary[:200]}. Style: modern digital art, dramatic lighting, "
            f"16:9 aspect ratio."
        ),
        "negative_prompt": "text, watermark, blurry, low quality",
        "source_url": item.url,
    }


def generate_video_script(item: ContentItem) -> Dict:
    """
    Produce a short-form video script with scene-by-scene visual/audio cues,
    suitable for handoff to a video editor.
    """
    return {
        "type": "video_script",
        "headline": item.title,
        "hook": f"You won't believe this: {item.title}",
        "scenes": [
            {
                "scene": 1,
                "visual": "Bold on-screen text with the hook, fast zoom-in",
                "audio": "Upbeat trending sound / voiceover reads the hook",
            },
            {
                "scene": 2,
                "visual": "B-roll / relevant footage or generated visuals",
                "audio": f"Voiceover: {item.summary[:150]}",
            },
            {
                "scene": 3,
                "visual": "Text overlay with key stat or quote",
                "audio": "Music swell",
            },
            {
                "scene": 4,
                "visual": "Call-to-action end card (follow/subscribe)",
                "audio": "Voiceover: closing line + CTA",
            },
        ],
        "estimated_length_seconds": 30,
        "source_url": item.url,
    }


GENERATORS = {
    "text_script": generate_text_script,
    "image_prompt": generate_image_prompt,
    "video_script": generate_video_script,
}


def generate_content(item: ContentItem) -> Dict:
    """Dispatch to the correct generator based on item.content_type."""
    if not item.content_type:
        classify_content(item)
    generator = GENERATORS[item.content_type]
    output = generator(item)
    item.generated_output = output
    return output


# ---------------------------------------------------------------------------
# 4. PACKAGING
# ---------------------------------------------------------------------------
def build_package(items: List[ContentItem]) -> str:
    """
    Assemble all generated ContentItems into a single, readable text package
    (hook + script/prompt + metadata) for the email body.
    """
    sections = ["AGENT 1 — CONTENT PACKAGE", "=" * 40, ""]

    for i, item in enumerate(items, start=1):
        out = item.generated_output
        sections.append(f"[{i}] {item.title}  ({item.content_type})")
        sections.append(f"Source: {item.source} | {item.url}")
        sections.append("-" * 40)

        if item.content_type == "text_script":
            sections.append(f"Hook: {out['hook']}")
            sections.append("Outline:")
            for step in out["outline"]:
                sections.append(f"  - {step}")

        elif item.content_type == "image_prompt":
            sections.append(f"Prompt: {out['prompt']}")
            sections.append(f"Negative prompt: {out['negative_prompt']}")

        elif item.content_type == "video_script":
            sections.append(f"Hook: {out['hook']}")
            sections.append(f"Est. length: {out['estimated_length_seconds']}s")
            sections.append("Scenes:")
            for scene in out["scenes"]:
                sections.append(f"  Scene {scene['scene']}:")
                sections.append(f"    Visual: {scene['visual']}")
                sections.append(f"    Audio:  {scene['audio']}")

        sections.append("")  # blank line between items

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# 5. OUTGOING EMAIL — dispatch
# ---------------------------------------------------------------------------
def send_email(subject: str, body: str, to_address: Optional[str] = None) -> bool:
    """
    Send the finished package via SMTP (e.g. Gmail with an app password, or
    any SMTP-compatible provider). Returns True on success, False on failure.

    IMPORTANT: This function sends real email once credentials are configured.
    It is only invoked when main()/run_pipeline()/check_inbox_for_commands()
    explicitly call it — nothing in this file triggers it automatically on
    import or on a schedule.
    """
    to_address = to_address or CONFIG["EMAIL_TO"]

    if CONFIG["SMTP_PASSWORD"].startswith("YOUR_") or CONFIG["SMTP_USERNAME"].startswith("YOUR_"):
        logger.warning("SMTP credentials are not configured — email not sent. "
                        "Set SMTP_USERNAME / SMTP_PASSWORD / EMAIL_FROM / EMAIL_TO.")
        return False

    msg = MIMEMultipart()
    msg["From"] = CONFIG["EMAIL_FROM"]
    msg["To"] = to_address
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(CONFIG["SMTP_HOST"], CONFIG["SMTP_PORT"], context=context) as server:
            server.login(CONFIG["SMTP_USERNAME"], CONFIG["SMTP_PASSWORD"])
            server.sendmail(CONFIG["EMAIL_FROM"], to_address, msg.as_string())
        logger.info("Email sent to %s", to_address)
        return True
    except Exception as exc:
        logger.error("Failed to send email: %s", exc)
        return False


# ---------------------------------------------------------------------------
# 6. INBOX-COMMAND MODE — IMAP polling, parsing, and safe dispatch
# ---------------------------------------------------------------------------
#
# SECURITY MODEL — read this before changing anything below.
#
#   * Only senders in CONFIG["ALLOWED_COMMAND_SENDERS"] are considered at all.
#     Everything else is skipped and left unread (untouched).
#   * Only unread emails whose subject contains CONFIG["COMMAND_SUBJECT_MARKER"]
#     are treated as commands.
#   * From the body, ONLY the specific "Key: value" fields matched by
#     _COMMAND_FIELD_PATTERNS below are extracted, via regex. There is no
#     eval(), exec(), os.system(), or subprocess call anywhere in this
#     pipeline, and the extracted text is only ever used as plain strings /
#     a parsed JSON dict passed into the existing ContentItem / generator
#     functions above — never as code, a shell command, or a file path.
#   * "Constraints" is parsed with json.loads inside a try/except; if it is
#     not valid JSON it is simply ignored (kept as an empty dict), never
#     evaluated as Python.
#   * Every processed command is replied to and marked as read (IMAP marks a
#     fetched message \Seen automatically), so a message is never re-run.
#
_COMMAND_FIELD_PATTERNS = {
    "run_id": re.compile(r"^\s*Run ID:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    "topic": re.compile(r"^\s*Topic:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    "constraints": re.compile(r"^\s*Constraints:\s*(\{.*\})\s*$", re.IGNORECASE | re.MULTILINE),
    "content_type": re.compile(r"^\s*Content-Type:\s*(text_script|image_prompt|video_script)\s*$",
                                re.IGNORECASE | re.MULTILINE),
}


def _get_email_body(msg: email_lib.message.Message) -> str:
    """Extract the plain-text body of an email.message.Message."""
    if msg.is_multipart():
        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition", ""))
            if part.get_content_type() == "text/plain" and "attachment" not in content_disposition:
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
        return ""
    payload = msg.get_payload(decode=True)
    return payload.decode(errors="ignore") if payload else ""


def _parse_sender_address(raw_from: str) -> str:
    """Pull just the bare email address out of a 'Display Name <addr>' header."""
    match = re.search(r"<([^>]+)>", raw_from)
    return (match.group(1) if match else raw_from).strip().lower()


def parse_command_email(msg: email_lib.message.Message) -> Optional[InboxCommand]:
    """
    Parse a fetched email.message.Message into an InboxCommand, applying the
    sender allowlist and subject marker check. Returns None if the email
    should not be treated as a command (wrong sender, no marker, or no
    parseable Topic field).
    """
    from_address = _parse_sender_address(msg.get("From", ""))
    subject = msg.get("Subject", "") or ""

    allowed = CONFIG["ALLOWED_COMMAND_SENDERS"]
    if allowed and from_address not in allowed:
        logger.info("Ignoring email from non-allow-listed sender: %s", from_address)
        return None

    if CONFIG["COMMAND_SUBJECT_MARKER"].lower() not in subject.lower():
        logger.info("Ignoring email (subject missing command marker): %r", subject)
        return None

    body = _get_email_body(msg)

    topic_match = _COMMAND_FIELD_PATTERNS["topic"].search(body)
    if not topic_match:
        logger.warning("Command-marked email from %s had no parseable 'Topic:' field — skipping.",
                        from_address)
        return None

    cmd = InboxCommand(from_address=from_address, subject=subject)
    cmd.topic = topic_match.group(1).strip()

    run_id_match = _COMMAND_FIELD_PATTERNS["run_id"].search(body)
    if run_id_match:
        cmd.run_id = run_id_match.group(1).strip()

    content_type_match = _COMMAND_FIELD_PATTERNS["content_type"].search(body)
    if content_type_match:
        cmd.content_type = content_type_match.group(1).strip().lower()

    constraints_match = _COMMAND_FIELD_PATTERNS["constraints"].search(body)
    if constraints_match:
        try:
            parsed = json.loads(constraints_match.group(1))
            if isinstance(parsed, dict):
                cmd.constraints = parsed
        except (json.JSONDecodeError, ValueError):
            logger.warning("Could not parse Constraints JSON from %s — ignoring constraints.",
                            from_address)

    logger.info("Parsed command from %s: run_id=%r topic=%r content_type=%r constraints=%s",
                from_address, cmd.run_id, cmd.topic, cmd.content_type, cmd.constraints)
    return cmd


def fetch_unread_command_emails() -> List[Tuple[InboxCommand, email_lib.message.Message]]:
    """
    Connect to the configured IMAP inbox, fetch UNSEEN messages, and return the
    ones that parse into a valid InboxCommand (sender allow-listed, subject
    marker present, Topic field present). Non-matching unread mail is left
    untouched (not marked read, not acted on).
    """
    if CONFIG["IMAP_PASSWORD"].startswith("YOUR_") or CONFIG["IMAP_USERNAME"].startswith("YOUR_"):
        logger.warning("IMAP credentials are not configured — skipping inbox check.")
        return []

    if not CONFIG["ALLOWED_COMMAND_SENDERS"]:
        logger.warning("ALLOWED_COMMAND_SENDERS is empty — refusing to process inbox commands "
                        "until at least one trusted sender is configured.")
        return []

    commands: List[Tuple[InboxCommand, email_lib.message.Message]] = []

    try:
        conn = imaplib.IMAP4_SSL(CONFIG["IMAP_HOST"], CONFIG["IMAP_PORT"])
        conn.login(CONFIG["IMAP_USERNAME"], CONFIG["IMAP_PASSWORD"])
        conn.select(CONFIG["IMAP_MAILBOX"])

        status, data = conn.search(None, "UNSEEN")
        if status != "OK":
            logger.warning("IMAP search failed: %s", status)
            conn.logout()
            return []

        message_ids = data[0].split()
        logger.info("Found %d unread message(s) in %s", len(message_ids), CONFIG["IMAP_MAILBOX"])

        for msg_id in message_ids:
            if len(commands) >= CONFIG["MAX_COMMANDS_PER_CHECK"]:
                break
            # Fetching with RFC822 also marks the message \Seen, which is what
            # prevents the same command from being processed twice.
            status, msg_data = conn.fetch(msg_id, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue
            msg = email_lib.message_from_bytes(msg_data[0][1])
            parsed = parse_command_email(msg)
            if parsed:
                commands.append((parsed, msg))

        conn.logout()
    except (imaplib.IMAP4.error, OSError, TimeoutError) as exc:
        logger.error("IMAP connection/search error: %s", exc)
        return []

    return commands


def process_inbox_command(cmd: InboxCommand) -> ContentItem:
    """
    Turn a parsed InboxCommand into a generated ContentItem, using the SAME
    classify_content()/generate_* functions used by the RSS/News pipeline.
    No code from the email is executed — cmd.topic is used only as plain text
    for the ContentItem title/summary.
    """
    item = ContentItem(
        title=cmd.topic,
        summary=f"Requested via email command (run_id={cmd.run_id or 'n/a'}). "
                f"Constraints: {json.dumps(cmd.constraints)}" if cmd.constraints else
                f"Requested via email command (run_id={cmd.run_id or 'n/a'}).",
        source=f"inbox-command:{cmd.from_address}",
    )

    if cmd.content_type in GENERATORS:
        item.content_type = cmd.content_type
    else:
        classify_content(item)

    generate_content(item)
    return item


def reply_to_command(cmd: InboxCommand, item: ContentItem) -> bool:
    """
    Email the generated content back to whoever sent the command, preserving
    the run_id in the subject so an orchestrator (e.g. a Lead Director agent)
    can match it to the run it is polling for.
    """
    subject = f"Re: {cmd.subject}" if not cmd.subject.lower().startswith("re:") else cmd.subject
    if cmd.run_id and cmd.run_id not in subject:
        subject = f"{subject} [{cmd.run_id}]"

    body = build_package([item])
    return send_email(subject=subject, body=body, to_address=cmd.from_address)


def check_inbox_for_commands() -> List[ContentItem]:
    """
    Single-pass inbox check: fetch allow-listed, marker-matching unread
    command emails, generate content for each, and reply. Does not loop —
    call this from cron / a scheduler / an external polling loop for
    recurring behavior (mirrors run_pipeline()'s single-pass design).
    """
    processed: List[ContentItem] = []
    commands = fetch_unread_command_emails()

    if not commands:
        logger.info("No new inbox commands to process.")
        return processed

    for cmd, _raw_msg in commands:
        try:
            item = process_inbox_command(cmd)
            reply_to_command(cmd, item)
            processed.append(item)
        except Exception as exc:
            logger.error("Failed to process command from %s (run_id=%s): %s",
                         cmd.from_address, cmd.run_id, exc)

    logger.info("Processed %d inbox command(s).", len(processed))
    return processed


def poll_inbox_loop(interval_seconds: int = 60, max_iterations: Optional[int] = None):
    """
    Optional convenience loop around check_inbox_for_commands(), for
    deployments that want an always-on process instead of an external cron
    schedule. Blocking — run in its own process/thread/service.

    max_iterations is provided mainly for testing; leave it as None to poll
    indefinitely.
    """
    iterations = 0
    logger.info("Starting inbox polling loop (interval=%ds)...", interval_seconds)
    while max_iterations is None or iterations < max_iterations:
        try:
            check_inbox_for_commands()
        except Exception as exc:
            logger.error("Unhandled error during inbox poll: %s", exc)
        iterations += 1
        if max_iterations is None or iterations < max_iterations:
            time.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# ORCHESTRATION — single-pass RSS/News pipeline (original mode)
# ---------------------------------------------------------------------------
def run_pipeline() -> List[ContentItem]:
    """
    Runs ONE pass of: fetch -> classify -> generate -> package -> email.
    Does not loop, does not schedule itself. Call this from cron, a cloud
    function trigger, or a workflow orchestrator for recurring execution.
    """
    raw_items = fetch_rss_feeds() + fetch_news_api()

    if not raw_items:
        logger.warning("No items fetched this run — nothing to process.")
        return []

    selected = raw_items[: CONFIG["MAX_ITEMS_PER_RUN"]]

    for item in selected:
        classify_content(item)
        generate_content(item)

    package_body = build_package(selected)
    send_email(subject="Agent 1 — New Content Package", body=package_body)

    return selected


def main():
    """
    Entry point. Set AGENT1_MODE to choose behavior:
      "pipeline" (default) -> run_pipeline(): fetch RSS/News, generate, email.
      "inbox"              -> check_inbox_for_commands(): single inbox check.
      "inbox_loop"         -> poll_inbox_loop(): blocking, always-on polling.
    """
    mode = os.getenv("AGENT1_MODE", "pipeline").strip().lower()
    logger.info("Starting Agent 1 in mode=%s...", mode)

    if mode == "pipeline":
        results = run_pipeline()
        logger.info("Pipeline run complete. %d items processed.", len(results))
    elif mode == "inbox":
        results = check_inbox_for_commands()
        logger.info("Inbox check complete. %d commands processed.", len(results))
    elif mode == "inbox_loop":
        interval = int(os.getenv("AGENT1_POLL_INTERVAL_SECONDS", "60"))
        poll_inbox_loop(interval_seconds=interval)
    else:
        logger.error("Unknown AGENT1_MODE=%r (expected pipeline|inbox|inbox_loop)", mode)


if __name__ == "__main__":
    main()
