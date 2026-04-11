import pandas as pd
import re
import os
import unicodedata
from datetime import datetime
from collections import defaultdict
from PyPDF2 import PdfReader, PdfWriter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT


# -------------------------
# SKU NORMALIZE + MATCH (keyboard / Unicode safe)
# -------------------------
def normalize_sku_key(text):
    """Normalize for comparison: strip, Unicode NFC, collapse spaces, casefold.
    Keeps letters, digits, punctuation, symbols — anything you can type (no stripping
    to alphanumeric-only, so SKUs like '3016 - KUMKUM', 'S48@#x', Hindi+ASCII mix work)."""
    if text is None:
        return ""
    s = unicodedata.normalize("NFC", str(text)).strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def clean_text(text):
    """Backward-compatible alias for normalize_sku_key."""
    return normalize_sku_key(text)


def _mostly_digits(s):
    s = s.replace(" ", "")
    if not s:
        return False
    return all(ch.isdigit() for ch in s)


# Manifest must share this much of the longer string's length (stops "rangoli" → "rangoli_orange").
_MIN_SKU_OVERLAP_RATIO = 0.72
# Min length for digit-only substring containment (avoids "191" inside "1910715226").
_MIN_DIGIT_SUB_LEN = 5
# If Excel variant is a prefix of manifest text, allow only this many extra characters (suffix).
_MAX_MANIFEST_SUFFIX_OVER_VARIANT = 14


def variants_match(norm_variant, norm_manifest):
    """True if training variant matches manifest SKU after normalize_sku_key.

    Digit SKUs: equality or digit substring with minimum length.
    Text: high overlap ratio, or Excel variant is a long-enough prefix of manifest with short suffix.
    """
    nv, nm = norm_variant, norm_manifest
    if not nv or not nm:
        return False
    if nv == nm:
        return True

    v0, m0 = nv.replace(" ", ""), nm.replace(" ", "")

    if _mostly_digits(nv) or _mostly_digits(nm):
        if v0 == m0:
            return True
        if _mostly_digits(nv) and len(v0) >= _MIN_DIGIT_SUB_LEN and v0 in m0:
            return True
        if _mostly_digits(nm) and len(m0) >= _MIN_DIGIT_SUB_LEN and m0 in v0:
            return True
        return False

    # Excel cell contained in manifest (manifest is longer or same).
    if nv in nm:
        if len(nv) >= _MIN_SKU_OVERLAP_RATIO * len(nm):
            return True
        if (
            len(nv) >= 10
            and nm.startswith(nv)
            and (len(nm) - len(nv)) <= _MAX_MANIFEST_SUFFIX_OVER_VARIANT
        ):
            return True

    # Manifest / picklist text contained in Excel training cell (Excel is longer).
    if nm in nv:
        if len(nm) >= _MIN_SKU_OVERLAP_RATIO * len(nv):
            return True

    return False


# -------------------------
# TRAIN EXCEL (FOR REPORT)
# -------------------------
def train_from_excel(file_path):
    df = pd.read_excel(file_path, header=None)

    main_row = df.iloc[0]
    sub_row = df.iloc[1]

    mapping = []

    for col in df.columns:
        main = str(main_row[col]).strip()
        sub = str(sub_row[col]).strip()

        if main == 'nan' or sub == 'nan':
            continue

        variants = df.iloc[2:, col].dropna().tolist()
        norm_variants = []
        for v in variants:
            k = normalize_sku_key(v)
            if k and k not in norm_variants:
                norm_variants.append(k)
        # Longest variants first so overlapping aliases resolve to the most specific row.
        norm_variants.sort(key=len, reverse=True)

        mapping.append({
            "main": main.upper(),
            "sub": sub.upper(),
            "variants": norm_variants,
        })

    return mapping


# -------------------------
# MANIFEST PDF EXTRACT (picklist + courier layouts)
# -------------------------
_HEADER_RE = re.compile(
    r'^\s*(Picklist|Supplier\s+Name|Date\s*:|SKU\s+Color|S\.\s*No\.|Sub\s+Order|'
    r'AWB|Courier\s*:|Total\s+Quantity|Qty\.?\s*Size|Packed)\b',
    re.I,
)


def _strip_logistics_prefix(prefix):
    """Remove leading S.No / order / AWB tokens. Keep a lone long numeric token — that is often the SKU."""
    parts = prefix.split()
    i = 0
    n = len(parts)
    while i < n:
        p = parts[i]
        remaining = n - i

        if p.isdigit() and len(p) <= 3:
            i += 1
            continue
        # Long digit chunk: only treat as logistics ID if something follows (AWB before real SKU).
        if p.isdigit() and len(p) >= 9:
            if remaining > 1:
                i += 1
                continue
            break
        if re.fullmatch(r'\d+_\d+', p):
            if remaining > 1:
                i += 1
                continue
            break
        if re.match(r'^VL\d+$', p, re.I) or re.match(r'^SF[A-Z0-9]+$', p, re.I):
            if remaining > 1:
                i += 1
                continue
            break
        break
    return ' '.join(parts[i:]).strip()


def _try_picklist_line(line):
    """… <SKU tokens> <color word> Free Size <qty> — SKU may contain any printable chars."""
    m = re.search(r"^(.+)\s+Free\s+Size\s+(\d+)\s*$", line.strip(), re.I)
    if not m:
        return None
    body, qty_s = m.group(1).strip(), m.group(2)
    if not body:
        return None
    parts = body.split()
    if not parts:
        return None
    # Last token is treated as colour; everything before is the SKU (multi-word OK).
    sku = parts[0] if len(parts) == 1 else " ".join(parts[:-1]).strip()
    low = sku.casefold()
    if low in ("sku", "total", "quantity", "size", "packed") or not sku:
        return None
    return sku, int(qty_s)


def _try_courier_tail(line):
    """... sku <qty> Free Size at end (Delhivery / Shadowfax / Valmo tables)."""
    m = re.search(r'(.+)\s+(\d{1,7})\s+Free\s*Size\s*$', line, re.I)
    if not m:
        return None
    prefix, qty_s = m.group(1).strip(), m.group(2)
    if int(qty_s) < 1:
        return None
    sku = _strip_logistics_prefix(prefix)
    if not sku:
        return None
    # Numeric-only SKUs (e.g. 1910715226) are valid — do not drop them.
    return sku, int(qty_s)


def _try_simple_tail_qty(line):
    """Last resort: trailing space + integer qty (no 'Free Size' on line). SKU = rest of line."""
    if "free" in line.lower():
        return None
    line = line.strip()
    m = re.search(r"\s+(\d{1,10})\s*$", line)
    if not m:
        return None
    sku = line[: m.start()].strip()
    if len(sku) < 1:
        return None
    if not re.search(r"[^\s]", sku):
        return None
    return sku, int(m.group(1))


def extract_line_sku_qty(line):
    """Return (sku, qty) or None for one text line."""
    line = (line or '').strip()
    if not line or len(line) < 4:
        return None
    if _HEADER_RE.match(line):
        return None
    if re.fullmatch(r'\(\d+\)', line):
        return None

    for fn in (_try_picklist_line, _try_courier_tail, _try_simple_tail_qty):
        got = fn(line)
        if got:
            return got
    return None


def _merge_broken_pdf_lines(lines):
    """Join SKU-only line with following 'qty Free Size' when PDF breaks one row."""
    out = []
    i = 0
    while i < len(lines):
        cur = (lines[i] or "").strip()
        nxt = (lines[i + 1] or "").strip() if i + 1 < len(lines) else ""
        if cur and nxt and extract_line_sku_qty(cur) is None:
            if re.match(r"^\d{1,10}\s+Free\s*Size", nxt, re.I):
                merged = f"{cur} {nxt}"
                if extract_line_sku_qty(merged):
                    out.append(merged)
                    i += 2
                    continue
        if cur:
            out.append(cur)
        i += 1
    return out


def extract_from_pdf(pdf_path):
    data = []
    reader = PdfReader(pdf_path)

    for page in reader.pages:
        text = page.extract_text() or ""
        raw_lines = text.split("\n")
        lines = _merge_broken_pdf_lines(raw_lines)

        for line in lines:
            got = extract_line_sku_qty(line)
            if got:
                data.append(got)

    return data


# -------------------------
# MATCH REPORT
# -------------------------
def match_and_group(mapping, manifest_data):
    """Map each manifest line to the best Excel column (longest matching variant wins)."""
    result = defaultdict(lambda: defaultdict(int))

    for raw_sku, qty in manifest_data:
        nm = normalize_sku_key(raw_sku)
        if not nm:
            continue

        best_main, best_sub, best_len = None, None, -1
        for item in mapping:
            for v in item["variants"]:
                if not v:
                    continue
                if variants_match(v, nm):
                    if len(v) > best_len:
                        best_len = len(v)
                        best_main = item["main"]
                        best_sub = item["sub"]

        if best_main is not None:
            result[best_main][best_sub] += qty

    return result


# -------------------------
# REPORT PDF
# -------------------------
def generate_pdf(result, output_path):
    doc = SimpleDocTemplate(output_path)
    styles = getSampleStyleSheet()

    title = ParagraphStyle(
        "title",
        parent=styles["Title"],
        alignment=TA_LEFT,
        fontSize=20,
        leading=26,
        spaceAfter=8
    )

    normal = ParagraphStyle(
        "normal",
        parent=styles["Normal"],
        alignment=TA_LEFT,
        fontSize=16,
        leading=22,
        spaceAfter=6
    )

    elements = []
    total = 0

    for main, subs in result.items():
        elements.append(Paragraph(f"<b>{main}</b>", title))

        for sub, qty in subs.items():
            elements.append(Paragraph(f"{sub} → {qty}", normal))
            total += qty

        elements.append(Spacer(1, 16))

    total_style = ParagraphStyle(
        "total",
        parent=styles["Title"],
        alignment=TA_LEFT,
        fontSize=22,
        leading=28,
        spaceBefore=10
    )

    elements.append(Paragraph(f"<b>Total: {total}</b>", total_style))

    doc.build(elements)


# =========================================================
# 🔥 LABEL SORT LOGIC (FULL FIXED COURIER SYSTEM)
# =========================================================

def normalize_courier(text):
    return re.sub(r"\s+", "", str(text).lower())


def extract_label_data(text):
    if not text:
        return None, 1, "Other"

    text = re.sub(r"\s+", " ", text).strip()

    # -------------------------
    # QUANTITY
    # -------------------------
    qty = 1
    match = re.search(r"Free\s*Size\s*(\d+)", text, re.IGNORECASE)
    if match:
        qty = int(match.group(1))

    # -------------------------
    # COURIER DETECTION (FIXED)
    # -------------------------
    # Longer keys first so "valmoplus" is not misread as "valmo".
    partners = [
        ("valmoplus", "ValmoPlus"),
        ("ecomexpress", "Ecom Express"),
        ("xpressbees", "Xpress Bees"),
        ("delhivery", "Delhivery"),
        ("shadowfax", "Shadowfax"),
        ("valmo", "Valmo"),
    ]

    courier = "Other"
    clean_text_courier = normalize_courier(text)

    for key, value in partners:
        if key in clean_text_courier:
            courier = value
            break

    # -------------------------
    # SKU EXTRACTION
    # -------------------------
    sku = None

    order_match = re.search(
        r"Order\s*No\.?\s*(.*?)\s*(Free\s*Size|Size)",
        text,
        re.IGNORECASE | re.DOTALL
    )

    if order_match:
        sku = " ".join(order_match.group(1).split())

    if not sku:
        sku_match = re.search(r"SKU\s*(.*?)\s*Size", text, re.IGNORECASE | re.DOTALL)
        if sku_match:
            sku = " ".join(sku_match.group(1).split())

    return sku, qty, courier


# -------------------------
# SORT LOGIC
# -------------------------
def get_sorted_indices(pages, df):
    final = []
    used = set()

    for p in pages:
        if p["qty"] > 1:
            final.append(p["index"])
            used.add(p["index"])

    for col in df.columns:
        skus = df[col].dropna().astype(str).str.strip().tolist()

        for sku in skus:
            for p in pages:
                if p["index"] not in used and p["sku"]:
                    if p["sku"].lower() == sku.lower():
                        final.append(p["index"])
                        used.add(p["index"])

    for p in pages:
        if p["index"] not in used:
            final.append(p["index"])

    return final


# -------------------------
# MAIN SORT PIPELINE
# -------------------------
def process_sort_pipeline(pdf_path, excel_path, selected_couriers=None, output_dir="output"):

    reader = PdfReader(pdf_path)
    df = pd.read_excel(excel_path)

    all_pages = []

    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""

        sku, qty, courier = extract_label_data(text)

        all_pages.append({
            "index": i,
            "sku": sku,
            "qty": qty,
            "courier": courier
        })

    # -------------------------
    # FILTER COURIERS (FIXED)
    # -------------------------
    if selected_couriers:
        selected_set = set(normalize_courier(c) for c in selected_couriers)

        pages = [
            p for p in all_pages
            if normalize_courier(p["courier"]) in selected_set
        ]
    else:
        pages = all_pages

    if not pages:
        raise Exception("❌ No matching labels found for selected couriers")

    # -------------------------
    # SORT
    # -------------------------
    indices = get_sorted_indices(pages, df)

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S_%f")

    if not selected_couriers:
        name = f"labels_ALL_{stamp}.pdf"
    else:
        part = "_".join([c.replace(" ", "") for c in selected_couriers])
        name = f"labels_{part}_{stamp}.pdf"

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, name)

    writer = PdfWriter()

    for idx in indices:
        if idx < len(reader.pages):
            writer.add_page(reader.pages[idx])

    if len(writer.pages) == 0:
        raise Exception("❌ No pages written (possible extraction issue)")

    with open(output_path, "wb") as f:
        writer.write(f)

    return output_path