import re
import unicodedata

from src.utils.logger import get_logger

log = get_logger(__name__)

MIN_TRANSCRIPT_CHARS = 500

BOILERPLATE_PATTERNS = [
    r"safe harbor",
    r"forward.looking statements?",
    r"actual results.{0,60}differ materially",
    r"securities and exchange commission",
    r"this transcript.{0,80}provided for informational",
    r"please stand by.{0,80}conference",
    r"your lines have been placed on mute",
    r"ladies and gentlemen.{0,40}welcome",
    r"operator instructions",
    r"\[operator\]",
    r"thank you for (standing by|joining)",
    r"all participants.{0,40}listen.only mode",
]

REMARKS_MARKERS = [
    "prepared remarks",
    "opening remarks",
    "management remarks",
    "formal remarks",
    "introductory remarks",
]

QA_MARKERS = [
    "question.and.answer",
    "q&a session",
    "q & a session",
    "questions and answers",
    "now open.{0,20}questions",
    "we will now begin.{0,20}question",
    "first question",
    "open the floor.{0,20}questions",
]

HTML_TAG_RE = re.compile(r"<[^>]+>")
SGML_HEADER_RE = re.compile(
    r"<SEC-DOCUMENT>.*?</SEC-HEADER>", re.DOTALL | re.IGNORECASE
)
XBRL_RE = re.compile(r"<XBRL>.*", re.DOTALL | re.IGNORECASE)
MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")
MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


def _strip_sgml(text):
    text = SGML_HEADER_RE.sub("", text)
    text = XBRL_RE.sub("", text)
    text = HTML_TAG_RE.sub(" ", text)
    return text


def _normalize(text):
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = MULTI_SPACE_RE.sub(" ", text)
    text = MULTI_NEWLINE_RE.sub("\n\n", text)
    return text.strip()


def _remove_boilerplate(text):
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        lower = line.lower()
        if any(re.search(p, lower) for p in BOILERPLATE_PATTERNS):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def _split_sections(text):
    lower = text.lower()

    qa_pos = None
    for marker in QA_MARKERS:
        match = re.search(marker, lower)
        if match:
            qa_pos = match.start()
            break

    if qa_pos is not None:
        remarks = text[:qa_pos].strip()
        qa = text[qa_pos:].strip()
    else:
        remarks = text
        qa = ""

    return remarks, qa


def _quality_flags(raw, remarks, qa):
    flags = []
    total_len = len(raw)

    if total_len < MIN_TRANSCRIPT_CHARS:
        flags.append("too_short")
    if not remarks:
        flags.append("no_remarks_section")
    if not qa:
        flags.append("no_qa_section")
    if "<SEC-DOCUMENT>" in raw or "<XBRL>" in raw:
        flags.append("contains_sgml_artifacts")
    if re.search(r"[^\x00-\x7F]{20,}", raw):
        flags.append("encoding_anomaly")

    return "|".join(flags) if flags else "ok"


def clean_transcript(raw_text):
    if not raw_text or not isinstance(raw_text, str):
        return {
            "cleaned_text": "",
            "remarks": "",
            "qa": "",
            "char_count": 0,
            "quality_flag": "missing_text",
        }

    text = _strip_sgml(raw_text)
    text = _normalize(text)
    text = _remove_boilerplate(text)
    remarks, qa = _split_sections(text)
    flags = _quality_flags(raw_text, remarks, qa)

    if flags != "ok":
        log.debug("Transcript quality flags: %s", flags)

    return {
        "cleaned_text": text,
        "remarks": remarks,
        "qa": qa,
        "char_count": len(text),
        "quality_flag": flags,
    }
