import pandas as pd
import re
import os
from datetime import datetime
from collections import defaultdict
from PyPDF2 import PdfReader, PdfWriter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT


# -------------------------
# CLEAN TEXT
# -------------------------
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r'[^a-z0-9]', '', text)
    text = re.sub(r'\d+$', '', text)
    return text


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
        clean_variants = [clean_text(v) for v in variants]

        mapping.append({
            "main": main.upper(),
            "sub": sub.upper(),
            "variants": clean_variants
        })

    return mapping


# -------------------------
# MANIFEST PDF EXTRACT
# -------------------------
def extract_from_pdf(pdf_path):
    data = []
    reader = PdfReader(pdf_path)

    for page in reader.pages:
        text = page.extract_text() or ""
        lines = text.split("\n")

        for line in lines:
            match = re.search(r'([A-Za-z0-9.\- ]+)\s+(\d+)$', line.strip())
            if match:
                data.append((match.group(1).strip(), int(match.group(2))))

    return data


# -------------------------
# MATCH REPORT
# -------------------------
def match_and_group(mapping, manifest_data):
    result = defaultdict(lambda: defaultdict(int))

    for raw_sku, qty in manifest_data:
        cleaned = clean_text(raw_sku)

        for item in mapping:
            matched = False
            for v in item["variants"]:
                if v and v in cleaned:
                    result[item["main"]][item["sub"]] += qty
                    matched = True
                    break
            if matched:
                break  # stop checking other groups

    return result


# -------------------------
# REPORT PDF
# -------------------------
def generate_pdf(result, output_path):
    doc = SimpleDocTemplate(output_path)
    styles = getSampleStyleSheet()

    # 🔥 TITLE STYLE
    title = ParagraphStyle(
        "title",
        parent=styles["Title"],
        alignment=TA_LEFT,
        fontSize=20,
        leading=26,   # 👈 line spacing
        spaceAfter=8  # 👈 space after heading
    )

    # 🔥 NORMAL TEXT STYLE
    normal = ParagraphStyle(
        "normal",
        parent=styles["Normal"],
        alignment=TA_LEFT,
        fontSize=16,
        leading=22,     # 👈 line spacing
        spaceAfter=6    # 👈 space between lines 🔥
    )

    elements = []
    total = 0

    for main, subs in result.items():
        elements.append(Paragraph(f"<b>{main}</b>", title))

        for sub, qty in subs.items():
            elements.append(Paragraph(f"{sub} → {qty}", normal))
            total += qty

        elements.append(Spacer(1, 16))  # 👈 space between sections

    # 🔥 TOTAL STYLE
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
# 🔥 LABEL SORT LOGIC (FIXED + IMPROVED)
# =========================================================

def extract_label_data(text):
    if not text:
        return None, 1, "Other"

    text = text.strip()

    # -------------------------
    # QUANTITY
    # -------------------------
    qty = 1
    match = re.search(r"Free\s*Size\s*(\d+)", text, re.IGNORECASE)
    if match:
        qty = int(match.group(1))

    # -------------------------
    # COURIER DETECTION
    # -------------------------
    partners = ['Delhivery', 'Valmo', 'ValmoPlus', 'Ecom Express', 'Xpressbees', 'Shadowfax']
    courier = "Other"

    for p in partners:
        if p.lower() in text.lower():
            courier = p
            break

    # -------------------------
    # SKU EXTRACTION (ROBUST)
    # -------------------------
    sku = None

    # Method 1: Order No block
    order_match = re.search(
        r"Order\s*No\.?\s*(.*?)\s*(Free\s*Size|Size)",
        text,
        re.IGNORECASE | re.DOTALL
    )
    if order_match:
        sku = " ".join(order_match.group(1).split())

    # Method 2: SKU block fallback
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

    # 1. BULK FIRST
    for p in pages:
        if p["qty"] > 1:
            final.append(p["index"])
            used.add(p["index"])

    # 2. EXCEL PRIORITY
    for col in df.columns:
        skus = df[col].dropna().astype(str).str.strip().tolist()

        for sku in skus:
            for p in pages:
                if p["index"] not in used and p["sku"]:
                    if p["sku"].lower() == sku.lower():
                        final.append(p["index"])
                        used.add(p["index"])

    # 3. REMAINING
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

    # -------------------------
    # EXTRACT ALL LABEL DATA
    # -------------------------
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
    # FILTER COURIERS
    # -------------------------
    if selected_couriers:
        pages = [p for p in all_pages if p["courier"] in selected_couriers]
    else:
        pages = all_pages

    if not pages:
        raise Exception("❌ No matching labels found for selected couriers")

    # -------------------------
    # SORT
    # -------------------------
    indices = get_sorted_indices(pages, df)

    # -------------------------
    # OUTPUT NAME
    # -------------------------
    today = datetime.now().strftime("%Y-%m-%d")

    if not selected_couriers:
        name = f"labels_ALL_{today}.pdf"
    else:
        part = "_".join([c.replace(" ", "") for c in selected_couriers])
        name = f"labels_{part}_{today}.pdf"

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, name)

    # -------------------------
    # WRITE PDF (FIXED)
    # -------------------------
    writer = PdfWriter()

    for idx in indices:
        if idx < len(reader.pages):
            writer.add_page(reader.pages[idx])

    if len(writer.pages) == 0:
        raise Exception("❌ No pages written (possible extraction issue)")

    with open(output_path, "wb") as f:
        writer.write(f)

    return output_path