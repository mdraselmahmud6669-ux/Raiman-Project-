"""
Lead Director — Email-Based Multi-Agent Workflow Orchestrator
================================================================

Unlike a direct-import orchestrator (calling agent modules as Python
functions), this Director treats each sub-agent as a remote mailbox: it
communicates entirely by SENDING task-instruction emails and POLLING for
reply emails. This suits a deployment where Discovery/Production/Distribution
run as separate services, processes, or even separate teams/tools that only
expose an email interface.

Flow for one work item:

    1. INTAKE     : Director receives a command (voice transcript or typed text).
    2. PARSE      : Command is parsed into a structured Task (topic, constraints).
    3. DISCOVERY  : Director emails the Discovery agent's inbox with the task.
                    Polls for Discovery's reply email (the raw content findings).
    4. PRODUCTION : Director emails the Production agent's inbox with Discovery's
                    findings. Polls for Production's reply email (finished asset
                    description + attachment).
    5. APPROVAL   : Director emails the human user the produced package and
                    polls for an APPROVE / REVISE reply.
    6. DISTRIBUTE : On approval, Director emails the Distribution agent's inbox
                    with the approved package, instructing it to publish.
    7. REPORT     : Director logs/report a final summary of the run.

This script does NOT run continuously by itself — call `main()` once per
command (e.g. from a CLI, a webhook that receives voice-to-text output, or a
scheduler). The email polling loops inside each stage ARE blocking waits
bounded by a timeout; for serverless/short-lived environments, replace each
poll with an event-driven resume (e.g. an inbound-mail webhook that re-invokes
the Director at the correct stage) rather than an in-process loop.

Dependencies:
    pip install requests

Configuration:
    Fill in CONFIG below or set the corresponding environment variables.
"""

import os
import re
import ssl
import time
import json
import imaplib
import smtplib
import logging
import traceback
import email as email_lib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lead_director")

_error_handler = logging.FileHandler("director_errors.log")
_error_handler.setLevel(logging.ERROR)
_error_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_error_handler)


# ---------------------------------------------------------------------------
# CONFIG — fill in, or set as environment variables of the same name.
# ---------------------------------------------------------------------------
CONFIG = {
    # --- Director's own mailbox (used for both sending and polling) ---
    "IMAP_HOST": os.getenv("IMAP_HOST", "imap.gmail.com"),
    "IMAP_PORT": int(os.getenv("IMAP_PORT", "993")),
    "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", "465")),
    "DIRECTOR_EMAIL": os.getenv("DIRECTOR_EMAIL", "director@example.com"),
    "DIRECTOR_PASSWORD": os.getenv("DIRECTOR_PASSWORD", "YOUR_APP_PASSWORD_HERE"),

    # --- Sub-agent mailbox addresses ---
    "DISCOVERY_AGENT_EMAIL": os.getenv("DISCOVERY_AGENT_EMAIL", "discovery-agent@example.com"),
    "PRODUCTION_AGENT_EMAIL": os.getenv("PRODUCTION_AGENT_EMAIL", "production-agent@example.com"),
    "DISTRIBUTION_AGENT_EMAIL": os.getenv("DISTRIBUTION_AGENT_EMAIL", "distribution-agent@example.com"),

    # --- Human user / approver ---
    "USER_EMAIL": os.getenv("USER_EMAIL", "user@example.com"),

    # --- Polling behavior ---
    "POLL_INTERVAL_SECONDS": int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
    "DISCOVERY_TIMEOUT_SECONDS": int(os.getenv("DISCOVERY_TIMEOUT_SECONDS", "900")),      # 15 min
    "PRODUCTION_TIMEOUT_SECONDS": int(os.getenv("PRODUCTION_TIMEOUT_SECONDS", "1800")),   # 30 min
    "APPROVAL_TIMEOUT_SECONDS": int(os.getenv("APPROVAL_TIMEOUT_SECONDS", "3600")),       # 1 hour
    "DISTRIBUTION_TIMEOUT_SECONDS": int(os.getenv("DISTRIBUTION_TIMEOUT_SECONDS", "900")), # 15 min

    # --- Self-healing ---
    "MAX_RETRIES": int(os.getenv("MAX_RETRIES", "3")),
    "RETRY_BACKOFF_BASE_SECONDS": int(os.getenv("RETRY_BACKOFF_BASE", "2")),

    "ATTACHMENT_DIR": os.getenv("ATTACHMENT_DIR", "./director_attachments"),
}

os.makedirs(CONFIG["ATTACHMENT_DIR"], exist_ok=True)


# ---------------------------------------------------------------------------
# STATE MACHINE
# ---------------------------------------------------------------------------
class Stage(Enum):
    RECEIVED_COMMAND = "RECEIVED_COMMAND"
    DISCOVERY_SENT = "DISCOVERY_SENT"
    DISCOVERY_RECEIVED = "DISCOVERY_RECEIVED"
    PRODUCTION_SENT = "PRODUCTION_SENT"
    PRODUCTION_RECEIVED = "PRODUCTION_RECEIVED"
    APPROVAL_SENT = "APPROVAL_SENT"
    APPROVED = "APPROVED"
    REVISE_REQUESTED = "REVISE_REQUESTED"
    DISTRIBUTION_SENT = "DISTRIBUTION_SENT"
    DISTRIBUTION_CONFIRMED = "DISTRIBUTION_CONFIRMED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass
class Task:
    """A structured representation of the parsed command."""
    run_id: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    raw_command: str = ""
    topic: str = ""
    constraints: Dict = field(default_factory=dict)  # e.g. {"platform": "tiktok", "tone": "urgent"}


@dataclass
class DirectorState:
    task: Task
    stage: Stage = Stage.RECEIVED_COMMAND
    discovery_reply_body: str = ""
    production_reply_body: str = ""
    production_attachment_path: Optional[str] = None
    approval_decision: Optional[str] = None  # "APPROVE" | "REVISE" | None
    failure_reason: Optional[str] = None
    failed_stage: Optional[str] = None
    stage_timestamps: Dict[str, str] = field(default_factory=dict)
    retry_counts: Dict[str, int] = field(default_factory=dict)

    def mark(self, stage: Stage):
        self.stage = stage
        self.stage_timestamps[stage.value] = datetime.utcnow().isoformat()
        logger.info("[run=%s] Stage -> %s", self.task.run_id, stage.value)

    def fail(self, stage_name: str, reason: str):
        self.stage = Stage.FAILED
        self.failed_stage = stage_name
        self.failure_reason = reason
        self.stage_timestamps[Stage.FAILED.value] = datetime.utcnow().isoformat()
        logger.error("[run=%s] FAILED at %s: %s", self.task.run_id, stage_name, reason)


# ---------------------------------------------------------------------------
# 1. INTAKE + PARSE — voice/text command -> structured Task
# ---------------------------------------------------------------------------
def transcribe_voice_command(audio_file_path: str) -> str:
    """
    Placeholder for speech-to-text transcription. Wire this to a real STT
    service (e.g. a cloud speech API) — this function is not implemented
    here since no such API/key was provided.
    """
    logger.warning("transcribe_voice_command() is a placeholder — no STT service configured. "
                    "Wire this to your speech-to-text provider of choice.")
    raise NotImplementedError("Speech-to-text integration not configured.")


def parse_command(raw_command: str) -> Task:
    """
    Parse a natural-language command into a structured Task.

    This is a lightweight rule-based parser (regex/keyword matching). For more
    flexible natural-language understanding, replace the body of this function
    with a call to an LLM API of your choice, prompted to return structured
    JSON (topic + constraints) — swap in whichever provider/model you have
    access to and a valid key for.
    """
    task = Task(raw_command=raw_command)

    # Very simple heuristic extraction — replace with real NLU as needed.
    topic_match = re.search(r"(?:about|on|regarding)\s+(.+?)(?:\.|$)", raw_command, re.IGNORECASE)
    task.topic = topic_match.group(1).strip() if topic_match else raw_command.strip()

    platform_match = re.search(r"\b(tiktok|instagram|youtube|twitter|threads|facebook)\b",
                                raw_command, re.IGNORECASE)
    if platform_match:
        task.constraints["platform"] = platform_match.group(1).lower()

    urgency_match = re.search(r"\b(urgent|asap|priority)\b", raw_command, re.IGNORECASE)
    if urgency_match:
        task.constraints["priority"] = "high"

    logger.info("[run=%s] Parsed task: topic=%r constraints=%s", task.run_id, task.topic, task.constraints)
    return task


# ---------------------------------------------------------------------------
# SELF-HEALING — retry wrapper for transient errors (e.g. SMTP/IMAP timeouts)
# ---------------------------------------------------------------------------
class TransientError(Exception):
    """Raised for retryable failures (network timeouts, temporary SMTP/IMAP errors)."""
    pass


def with_retries(func, *args, stage_name: str, state: DirectorState, **kwargs):
    """Exponential-backoff retry wrapper. Non-transient exceptions propagate immediately."""
    attempt = 0
    max_retries = CONFIG["MAX_RETRIES"]

    while True:
        try:
            result = func(*args, **kwargs)
            state.retry_counts[stage_name] = attempt
            return result
        except TransientError as exc:
            attempt += 1
            state.retry_counts[stage_name] = attempt
            if attempt > max_retries:
                logger.error("[run=%s] %s exhausted retries: %s", state.task.run_id, stage_name, exc)
                raise
            backoff = CONFIG["RETRY_BACKOFF_BASE_SECONDS"] ** attempt
            logger.warning("[run=%s] %s transient error (attempt %d/%d): %s — retrying in %ds",
                           state.task.run_id, stage_name, attempt, max_retries, exc, backoff)
            time.sleep(backoff)
        except Exception as exc:
            logger.error("[run=%s] %s permanent error: %s\n%s",
                         state.task.run_id, stage_name, exc, traceback.format_exc())
            raise


# ---------------------------------------------------------------------------
# EMAIL PRIMITIVES — shared send/poll helpers
# ---------------------------------------------------------------------------
def _send_email(to_address: str, subject: str, body: str, attachment_path: Optional[str] = None):
    """Low-level SMTP send. Raises TransientError on network-ish failures."""
    if CONFIG["DIRECTOR_PASSWORD"].startswith("YOUR_"):
        logger.warning("Director email credentials not configured — email not sent (subject=%r).", subject)
        return

    msg = MIMEMultipart()
    msg["From"] = CONFIG["DIRECTOR_EMAIL"]
    msg["To"] = to_address
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(attachment_path)}"')
        msg.attach(part)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(CONFIG["SMTP_HOST"], CONFIG["SMTP_PORT"], context=context) as server:
            server.login(CONFIG["DIRECTOR_EMAIL"], CONFIG["DIRECTOR_PASSWORD"])
            server.sendmail(CONFIG["DIRECTOR_EMAIL"], to_address, msg.as_string())
        logger.info("Email sent to %s: %s", to_address, subject)
    except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected, TimeoutError, OSError) as exc:
        raise TransientError(f"SMTP transient failure: {exc}")
    except Exception as exc:
        # Auth failures, malformed addresses etc. are permanent — don't retry blindly.
        raise RuntimeError(f"SMTP permanent failure: {exc}")


def _poll_for_reply(from_address: str, subject_marker: str, timeout_seconds: int) -> Optional[email_lib.message.Message]:
    """
    Poll the Director's inbox for an unread reply from `from_address` whose
    subject contains `subject_marker` (typically the run_id, to disambiguate
    concurrent runs). Returns the parsed email.Message, or None on timeout.
    """
    if CONFIG["DIRECTOR_PASSWORD"].startswith("YOUR_"):
        logger.warning("Director email credentials not configured — cannot poll for replies.")
        return None

    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            conn = imaplib.IMAP4_SSL(CONFIG["IMAP_HOST"], CONFIG["IMAP_PORT"])
            conn.login(CONFIG["DIRECTOR_EMAIL"], CONFIG["DIRECTOR_PASSWORD"])
            conn.select("INBOX")

            status, data = conn.search(
                None, f'(UNSEEN FROM "{from_address}" SUBJECT "{subject_marker}")'
            )
            if status == "OK" and data[0]:
                latest_id = data[0].split()[-1]
                status, msg_data = conn.fetch(latest_id, "(RFC822)")
                msg = email_lib.message_from_bytes(msg_data[0][1])
                conn.logout()
                return msg

            conn.logout()

        except (imaplib.IMAP4.error, TimeoutError, OSError) as exc:
            logger.warning("Polling error (will retry): %s", exc)

        time.sleep(CONFIG["POLL_INTERVAL_SECONDS"])

    return None


def _extract_body_and_attachment(msg: email_lib.message.Message) -> Tuple[str, Optional[str]]:
    """Extract plain-text body and (if present) save the first attachment to disk."""
    body = ""
    asset_path = None

    if msg.is_multipart():
        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition", ""))
            if part.get_content_type() == "text/plain" and "attachment" not in content_disposition:
                body = part.get_payload(decode=True).decode(errors="ignore")
            elif "attachment" in content_disposition:
                filename = part.get_filename()
                if filename:
                    asset_path = os.path.join(CONFIG["ATTACHMENT_DIR"], filename)
                    with open(asset_path, "wb") as f:
                        f.write(part.get_payload(decode=True))
    else:
        body = msg.get_payload(decode=True).decode(errors="ignore")

    return body, asset_path


# ---------------------------------------------------------------------------
# 2. DIRECTOR -> DISCOVERY AGENT
# ---------------------------------------------------------------------------
def dispatch_to_discovery(state: DirectorState) -> bool:
    subject = f"[Task {state.task.run_id}] Discovery Request: {state.task.topic}"
    body = (
        f"Discovery Agent — new task.\n\n"
        f"Run ID: {state.task.run_id}\n"
        f"Topic: {state.task.topic}\n"
        f"Constraints: {json.dumps(state.task.constraints)}\n\n"
        f"Please research this topic and reply to this thread with your findings "
        f"(keep '{state.task.run_id}' in the subject line so the Director can match your reply)."
    )
    try:
        with_retries(_send_email, CONFIG["DISCOVERY_AGENT_EMAIL"], subject, body,
                     stage_name="dispatch_discovery", state=state)
    except Exception as exc:
        state.fail("dispatch_discovery", f"Could not send task to Discovery agent: {exc}")
        return False

    state.mark(Stage.DISCOVERY_SENT)
    return True


def await_discovery_reply(state: DirectorState) -> bool:
    msg = _poll_for_reply(
        CONFIG["DISCOVERY_AGENT_EMAIL"], state.task.run_id, CONFIG["DISCOVERY_TIMEOUT_SECONDS"]
    )
    if msg is None:
        state.fail("await_discovery", "No reply from Discovery agent within timeout window.")
        return False

    body, _ = _extract_body_and_attachment(msg)
    if not body.strip():
        state.fail("await_discovery", "Discovery agent reply was empty.")
        return False

    state.discovery_reply_body = body
    state.mark(Stage.DISCOVERY_RECEIVED)
    return True


# ---------------------------------------------------------------------------
# 3. DIRECTOR -> PRODUCTION AGENT
# ---------------------------------------------------------------------------
def dispatch_to_production(state: DirectorState) -> bool:
    subject = f"[Task {state.task.run_id}] Production Request: {state.task.topic}"
    body = (
        f"Production Agent — new task.\n\n"
        f"Run ID: {state.task.run_id}\n"
        f"Topic: {state.task.topic}\n"
        f"Constraints: {json.dumps(state.task.constraints)}\n\n"
        f"--- Discovery Findings ---\n{state.discovery_reply_body}\n\n"
        f"Please produce the finished asset (text/image/video as appropriate) and "
        f"reply to this thread with a summary and the asset attached "
        f"(keep '{state.task.run_id}' in the subject line)."
    )
    try:
        with_retries(_send_email, CONFIG["PRODUCTION_AGENT_EMAIL"], subject, body,
                     stage_name="dispatch_production", state=state)
    except Exception as exc:
        state.fail("dispatch_production", f"Could not send task to Production agent: {exc}")
        return False

    state.mark(Stage.PRODUCTION_SENT)
    return True


def await_production_reply(state: DirectorState) -> bool:
    msg = _poll_for_reply(
        CONFIG["PRODUCTION_AGENT_EMAIL"], state.task.run_id, CONFIG["PRODUCTION_TIMEOUT_SECONDS"]
    )
    if msg is None:
        state.fail("await_production", "No reply from Production agent within timeout window.")
        return False

    body, asset_path = _extract_body_and_attachment(msg)
    if not body.strip():
        state.fail("await_production", "Production agent reply was empty.")
        return False

    state.production_reply_body = body
    state.production_attachment_path = asset_path
    state.mark(Stage.PRODUCTION_RECEIVED)
    return True


# ---------------------------------------------------------------------------
# 4. DIRECTOR -> USER (approval)
# ---------------------------------------------------------------------------
def dispatch_approval_request(state: DirectorState) -> bool:
    subject = f"[Approval Needed {state.task.run_id}] {state.task.topic}"
    body = (
        f"A new content package is ready for your review.\n\n"
        f"Topic: {state.task.topic}\n\n"
        f"--- Production Summary ---\n{state.production_reply_body}\n\n"
        f"Reply to this email with exactly one word as the FIRST line:\n"
        f"  APPROVE   — publish this content as-is\n"
        f"  REVISE    — send back for changes\n"
        f"(Keep '{state.task.run_id}' in the subject line.)"
    )
    try:
        with_retries(
            _send_email, CONFIG["USER_EMAIL"], subject, body,
            state.production_attachment_path,
            stage_name="dispatch_approval", state=state,
        )
    except Exception as exc:
        state.fail("dispatch_approval", f"Could not send approval request to user: {exc}")
        return False

    state.mark(Stage.APPROVAL_SENT)
    return True


def await_approval_reply(state: DirectorState) -> bool:
    msg = _poll_for_reply(CONFIG["USER_EMAIL"], state.task.run_id, CONFIG["APPROVAL_TIMEOUT_SECONDS"])
    if msg is None:
        state.fail("await_approval", "No approval/revise reply from user within timeout window.")
        return False

    body, _ = _extract_body_and_attachment(msg)
    first_line = body.strip().splitlines()[0].strip().upper() if body.strip() else ""

    if first_line == "APPROVE":
        state.approval_decision = "APPROVE"
        state.mark(Stage.APPROVED)
        return True
    elif first_line == "REVISE":
        state.approval_decision = "REVISE"
        state.mark(Stage.REVISE_REQUESTED)
        state.fail("await_approval", "User requested revisions — halting before distribution.")
        return False
    else:
        state.fail("await_approval", f"Reply received but first line was not APPROVE/REVISE: {first_line!r}")
        return False


# ---------------------------------------------------------------------------
# 5. DIRECTOR -> DISTRIBUTION AGENT
# ---------------------------------------------------------------------------
def dispatch_to_distribution(state: DirectorState) -> bool:
    subject = f"[Task {state.task.run_id}] Publish Approved Content: {state.task.topic}"
    body = (
        f"Distribution Agent — approved content ready to publish.\n\n"
        f"Run ID: {state.task.run_id}\n"
        f"Topic: {state.task.topic}\n"
        f"Constraints: {json.dumps(state.task.constraints)}\n\n"
        f"--- Approved Package ---\n{state.production_reply_body}\n\n"
        f"This content has been approved by {CONFIG['USER_EMAIL']}. Please publish it "
        f"and reply to this thread confirming distribution results "
        f"(keep '{state.task.run_id}' in the subject line)."
    )
    try:
        with_retries(
            _send_email, CONFIG["DISTRIBUTION_AGENT_EMAIL"], subject, body,
            state.production_attachment_path,
            stage_name="dispatch_distribution", state=state,
        )
    except Exception as exc:
        state.fail("dispatch_distribution", f"Could not send task to Distribution agent: {exc}")
        return False

    state.mark(Stage.DISTRIBUTION_SENT)
    return True


def await_distribution_confirmation(state: DirectorState) -> bool:
    msg = _poll_for_reply(
        CONFIG["DISTRIBUTION_AGENT_EMAIL"], state.task.run_id, CONFIG["DISTRIBUTION_TIMEOUT_SECONDS"]
    )
    if msg is None:
        state.fail("await_distribution", "No confirmation from Distribution agent within timeout window.")
        return False

    body, _ = _extract_body_and_attachment(msg)
    if not body.strip():
        state.fail("await_distribution", "Distribution agent confirmation was empty.")
        return False

    logger.info("[run=%s] Distribution confirmation: %s", state.task.run_id, body[:300])
    state.mark(Stage.DISTRIBUTION_CONFIRMED)
    return True


# ---------------------------------------------------------------------------
# 6. FINAL REPORT
# ---------------------------------------------------------------------------
def build_final_report(state: DirectorState) -> str:
    lines = [
        f"LEAD DIRECTOR EXECUTION REPORT — Run {state.task.run_id}",
        "=" * 50,
        f"Command: {state.task.raw_command}",
        f"Topic: {state.task.topic}",
        f"Final stage: {state.stage.value}",
        "",
        "Stage timestamps:",
    ]
    for stage_name, ts in state.stage_timestamps.items():
        lines.append(f"  {stage_name}: {ts}")

    lines.append("")
    lines.append("Retry counts:")
    for stage_name, count in state.retry_counts.items():
        lines.append(f"  {stage_name}: {count}")

    if state.stage == Stage.FAILED:
        lines += ["", f"FAILURE at: {state.failed_stage}", f"Reason: {state.failure_reason}"]
    else:
        lines += ["", "Run completed successfully."]

    return "\n".join(lines)


def send_final_report(state: DirectorState):
    body = build_final_report(state)
    logger.info("Final report:\n%s", body)
    try:
        with_retries(
            _send_email, CONFIG["USER_EMAIL"],
            f"Director Run Report {state.task.run_id} — {state.stage.value}",
            body,
            stage_name="send_final_report", state=state,
        )
    except Exception as exc:
        logger.error("[run=%s] Could not send final report: %s", state.task.run_id, exc)


# ---------------------------------------------------------------------------
# LEAD DIRECTOR — main orchestration function
# ---------------------------------------------------------------------------
def lead_director(command_text: str) -> DirectorState:
    """
    Runs one full command-to-publish cycle, entirely via email exchanges with
    the three sub-agents and the human approver.
    """
    task = parse_command(command_text)
    state = DirectorState(task=task)
    logger.info("[run=%s] Lead Director starting for command: %r", task.run_id, command_text)

    try:
        if not dispatch_to_discovery(state):
            return _finalize(state)
        if not await_discovery_reply(state):
            return _finalize(state)

        if not dispatch_to_production(state):
            return _finalize(state)
        if not await_production_reply(state):
            return _finalize(state)

        if not dispatch_approval_request(state):
            return _finalize(state)
        if not await_approval_reply(state):
            return _finalize(state)

        if not dispatch_to_distribution(state):
            return _finalize(state)
        if not await_distribution_confirmation(state):
            return _finalize(state)

        state.mark(Stage.COMPLETE)
        return _finalize(state)

    except Exception as exc:
        state.fail("orchestrator", f"Unhandled exception: {exc}\n{traceback.format_exc()}")
        return _finalize(state)


def _finalize(state: DirectorState) -> DirectorState:
    send_final_report(state)
    return state


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
def main():
    """
    Example single-pass entry point. In real deployment, `command_text` would
    come from a text command (CLI/chat input) or from transcribe_voice_command()
    fed by an actual speech-to-text service.
    """
    example_command = os.getenv("DIRECTOR_COMMAND", "Create content about renewable energy trends for TikTok")
    final_state = lead_director(example_command)
    logger.info("Run finished with stage: %s", final_state.stage.value)


if __name__ == "__main__":
    main()
