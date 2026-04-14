
from __future__ import annotations

import html
import io
import json
import math
import re
from pathlib import Path
from typing import Any, Optional

import openpyxl
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Мой Товар", page_icon="📦", layout="wide")

APP_TITLE = "Мой Товар"
DEFAULT_DISCOUNT_1 = 12.0
DEFAULT_DISCOUNT_2 = 20.0
DEFAULT_TEMPLATE1_FOOTER = (
    "Цeна с НДC : +17%\n\n"
    "Работaeм по будням, c 10 дo 18:00. Самовывоз по адресу: Москва, ул. Сущёвский Вал, 5с20\n\n"
    "Еcли пoтрeбуeтся пepeсылкa - oтпpaвляeм толькo Авитo-Яндeкc, Авито-СДЭК или Авито-Авито. Отправляем без наценки."
)

CATALOG_COLUMN_ALIASES = {
    "article": ["Артикул", "артикул", "Код", "код", "sku", "article"],
    "name": ["Наименование", "Номенклатура", "Название", "name"],
    "price": ["Наша цена", "Цена", "Цена продажи", "price"],
    "qty": ["Наш склад", "Свободно", "Остаток", "Количество", "qty"],
}

PHOTO_COLUMN_ALIASES = {
    "article": ["Артикул", "артикул", "Код", "код", "sku", "article"],
    "photo_url": [
        "Фото", "Ссылка на фото", "URL фото", "photo", "image", "image_url",
        "photo_url", "url", "link", "picture", "картинка", "ссылка",
        "imag", "images"
    ],
}

AVITO_COLUMN_ALIASES = {
    "ad_id": ["Номер объявления", "ID объявления", "Номер"],
    "title": ["Название объявления", "Заголовок", "Название"],
    "price": ["Цена"],
    "url": ["Ссылка", "URL", "Ссылка на объявление", "Link"],
}

CYRILLIC_ARTICLE_TRANSLATION = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "а": "A", "в": "B", "е": "E", "к": "K", "м": "M", "н": "H", "о": "O", "р": "P", "с": "C", "т": "T", "у": "Y", "х": "X",
    "Ё": "E", "ё": "E",
})


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = re.sub(r"\s+", " ", str(value).strip())
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def normalize_article(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = text.translate(CYRILLIC_ARTICLE_TRANSLATION)
    return re.sub(r"[^A-Za-z0-9]", "", text).upper()


def extract_first_url(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    # Поддержка Excel-формулы HYPERLINK(...)
    if text.startswith("="):
        m = re.match(r'^=\s*(?:HYPERLINK|ГИПЕРССЫЛКА)\(\s*"([^"]+)"', text, flags=re.IGNORECASE)
        if m:
            return normalize_text(m.group(1))

    m = re.search(r'https?://[^\s"\'<>]+', text, flags=re.IGNORECASE)
    if not m:
        return ""

    url = m.group(0).strip()

    # Иногда после ссылки в ячейке идёт хвост вида: " ! alt : ..."
    url = re.sub(r'[!;,]+$', '', url).strip()

    # Обрезаем совсем явный мусор после картинки/файла, если он попал в строку.
    for stopper in [' ! ', ' alt :', ' title :', ' desc :', ' caption :']:
        pos = url.lower().find(stopper.strip().lower())
        if pos > 0:
            url = url[:pos].strip()

    return url


def compact_text(value: object) -> str:
    return re.sub(r"\s+", "", normalize_text(value)).upper()


def contains_text(value: object) -> str:
    return normalize_text(value).upper()


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        if pd.isna(value):
            return float(default)
    except Exception:
        pass
    if isinstance(value, str):
        txt = normalize_text(value).replace(" ", "").replace(",", ".")
        if not txt:
            return float(default)
        try:
            return float(txt)
        except Exception:
            return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def fmt_price(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    val = safe_float(value, 0.0)
    if float(val).is_integer():
        return f"{int(val):,}".replace(",", " ")
    return f"{val:,.2f}".replace(",", " ").replace(".", ",")


def fmt_qty(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    val = safe_float(value, 0.0)
    if float(val).is_integer():
        return str(int(val))
    return f"{val:,.2f}".replace(",", " ").replace(".", ",")


def round_up_to_100(value: float) -> int:
    return int(math.ceil(float(value) / 100.0) * 100)


def round_to_nearest_100(value: float) -> int:
    return int(math.floor(float(value) / 100.0 + 0.5) * 100)


def current_discount(price_mode: str, custom_discount: float) -> float:
    if price_mode == "-12%":
        return DEFAULT_DISCOUNT_1
    if price_mode == "-20%":
        return DEFAULT_DISCOUNT_2
    return max(0.0, float(custom_discount))


def current_price_label(price_mode: str, custom_discount: float) -> str:
    disc = current_discount(price_mode, custom_discount)
    if float(disc).is_integer():
        return f"Цена -{int(disc)}%"
    return f"Цена -{str(round(disc, 2)).replace('.', ',')}%"


def get_selected_price_raw(row: pd.Series, price_mode: str, round100: bool, custom_discount: float) -> float:
    disc = current_discount(price_mode, custom_discount)
    value = safe_float(row.get("sale_price", 0.0), 0.0) * (1 - disc / 100)
    return float(round_up_to_100(value)) if round100 else float(round(value, 2))


def tokenize_text(value: object) -> list[str]:
    text = normalize_text(value)
    if not text:
        return []
    return [t for t in re.split(r"[^A-Za-zА-Яа-я0-9]+", text.upper()) if t]


ARTICLE_PIECE_RE = re.compile(r"[A-Za-zА-Яа-я0-9._/-]{3,}")


def is_candidate_article_norm(norm: str) -> bool:
    if not norm:
        return False
    if len(norm) < 5:
        return False
    has_digit = any(ch.isdigit() for ch in norm)
    has_alpha = any(ch.isalpha() for ch in norm)
    return has_digit and has_alpha


def extract_article_candidates_from_text(text: object) -> list[str]:
    raw = normalize_text(text)
    if not raw:
        return []
    chunks = ARTICLE_PIECE_RE.findall(raw)
    out: list[str] = []
    seen: set[str] = set()
    for chunk in chunks:
        norm = normalize_article(chunk)
        if not is_candidate_article_norm(norm) or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def unique_norm_codes(items: list[object]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        norm = normalize_article(item)
        if not is_candidate_article_norm(norm) or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def build_row_compare_codes(article: object, name: object) -> list[str]:
    return unique_norm_codes([article, *extract_article_candidates_from_text(name)])


def build_compatible_price_lookup(compatible_df: pd.DataFrame | None) -> dict[str, dict[str, set[float]]]:
    lookup: dict[str, dict[str, set[float]]] = {}
    if compatible_df is None or compatible_df.empty:
        return lookup
    for _, row in compatible_df.iterrows():
        codes = build_row_compare_codes(row.get("article", ""), row.get("name", ""))
        if not codes:
            continue
        for pair in row.get("source_pairs", []) or []:
            source = str(pair.get("source", "") or "")
            price = safe_float(row.get(pair.get("price_col", "")), 0.0)
            qty = parse_qty_generic(row.get(pair.get("qty_col", "")))
            if not source or price <= 0 or qty <= 0:
                continue
            price_key = round(float(price), 2)
            for code in codes:
                lookup.setdefault(code, {}).setdefault(source, set()).add(price_key)
    return lookup


def merge_blocked_source_prices(codes: list[str], compatible_lookup: dict[str, dict[str, set[float]]]) -> dict[str, list[float]]:
    out: dict[str, set[float]] = {}
    for code in codes or []:
        for source, prices in compatible_lookup.get(code, {}).items():
            out.setdefault(source, set()).update(prices)
    return {source: sorted(values) for source, values in out.items() if values}


def is_blocked_by_compatible_price(row: pd.Series, source: str, price: float) -> bool:
    blocked_map = row.get("blocked_source_prices", {})
    if not isinstance(blocked_map, dict) or not blocked_map:
        return False
    blocked_prices = blocked_map.get(str(source), [])
    if not blocked_prices:
        return False
    price_key = round(float(price), 2)
    return any(abs(float(blocked) - price_key) < 0.01 for blocked in blocked_prices)


def filter_suspicious_low_offers(row: pd.Series, offers: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    sheet_name = normalize_text(row.get("sheet_name", ""))
    own_price = safe_float(row.get("sale_price"), 0.0)
    if sheet_name != "Сравнение" or len(offers) < 3:
        return offers, []

    prices = sorted(float(offer["price"]) for offer in offers if float(offer["price"]) > 0)
    if len(prices) < 3:
        return offers, []

    upper_half = prices[len(prices) // 2 :]
    if not upper_half:
        return offers, []

    ref_price = upper_half[len(upper_half) // 2]
    outlier_limit = ref_price * 0.35
    if own_price > 0:
        outlier_limit = min(outlier_limit, own_price * 0.35)

    kept: list[dict[str, Any]] = []
    hidden: list[str] = []
    for offer in offers:
        price = float(offer["price"])
        if price < outlier_limit:
            hidden.append(f"{offer['source']} {fmt_price(price)}")
            continue
        kept.append(offer)
    return kept, hidden


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        norm = normalize_text(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(item)
    return out


def split_query_parts(query: str) -> list[str]:
    parts: list[str] = []
    raw_chunks = re.split(r"[\n,;]+", query)
    for chunk in raw_chunks:
        chunk = normalize_text(chunk)
        if not chunk:
            continue
        if "/" in chunk:
            slash_parts = [normalize_text(x) for x in re.split(r"\s*/\s*", chunk) if normalize_text(x)]
            if len(slash_parts) > 1:
                parts.extend(slash_parts)
                continue
        space_parts = [normalize_text(x) for x in re.split(r"\s+", chunk) if normalize_text(x)]
        if len(space_parts) > 1 and all(len(normalize_article(x)) >= 3 for x in space_parts):
            parts.extend(space_parts)
        else:
            parts.append(chunk)
    return parts


def normalize_query_for_display(query: str) -> str:
    return "\n".join(split_query_parts(query))


def find_column(columns: list[str], aliases: list[str]) -> Optional[str]:
    lower_map = {str(col).strip().lower(): col for col in columns}
    for alias in aliases:
        hit = lower_map.get(alias.strip().lower())
        if hit:
            return hit
    for alias in aliases:
        a = alias.strip().lower()
        for col in columns:
            c = str(col).strip().lower()
            if a in c or c in a:
                return col
    return None


def detect_mapping(df: pd.DataFrame, aliases_map: dict[str, list[str]]) -> dict[str, Optional[str]]:
    return {key: find_column(list(df.columns), aliases) for key, aliases in aliases_map.items()}


def parse_qty_generic(value: Any) -> float:
    raw = normalize_text(value)
    compact = compact_text(value)
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw.replace(" ", "").replace(",", ".")))
    except Exception:
        pass

    mapping = {
        "+": 1.0,
        "++": 5.0,
        "+++": 10.0,
        "МАЛО": 1.0,
        "ЕСТЬ": 1.0,
        "СРЕДНЕ": 5.0,
        "СРЕДНЕЕ": 5.0,
        "СРЕДНИЙ": 5.0,
        "СРЕДНЯЯ": 5.0,
        "МНОГО": 10.0,
        "CALL": 0.0,
        "НЕТ": 0.0,
        "ПОДЗАКАЗ": 0.0,
        "ПОДЗАКАЗ": 0.0,
        "ЗАКАЗ": 0.0,
        "ОЖИДАЕТСЯ": 0.0,
    }
    for key, val in mapping.items():
        if key in compact:
            return val
    m = re.search(r"(\d+[\.,]?\d*)", raw)
    if m:
        try:
            return max(0.0, float(m.group(1).replace(",", ".")))
        except Exception:
            return 0.0
    return 0.0


def parse_excel_hyperlink_formula(value: object) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text.startswith("="):
        return "", ""
    m = re.match(r'^=\s*(?:HYPERLINK|ГИПЕРССЫЛКА)\(\s*"([^"]+)"\s*[;,]\s*"([^"]*)"\s*\)$', text, flags=re.IGNORECASE)
    if not m:
        return "", ""
    return m.group(1).strip(), m.group(2).strip()


def cell_display_and_url(cell) -> tuple[str, str]:
    url = ""
    display = ""
    if cell is None:
        return display, url
    try:
        if getattr(cell, "hyperlink", None):
            url = str(cell.hyperlink.target or "").strip()
    except Exception:
        pass
    formula_url, formula_display = parse_excel_hyperlink_formula(cell.value)
    if formula_url:
        url = formula_url
        display = formula_display
    else:
        display = normalize_text(cell.value)
    return display, url


@st.cache_data(show_spinner=False)
def load_comparison_workbook(file_name: str, file_bytes: bytes) -> dict[str, pd.DataFrame]:
    wb = pd.ExcelFile(io.BytesIO(file_bytes))
    sheets: dict[str, pd.DataFrame] = {}
    for sheet in wb.sheet_names:
        raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet)
        raw = raw.dropna(how="all")
        mapping = detect_mapping(raw, CATALOG_COLUMN_ALIASES)
        required = ["article", "name", "price", "qty"]
        missing = [k for k in required if not mapping.get(k)]
        if missing:
            continue

        df = raw.copy()
        df["article"] = df[mapping["article"]].map(normalize_text)
        df["article_norm"] = df[mapping["article"]].map(normalize_article)
        df["name"] = df[mapping["name"]].map(normalize_text)
        df["sale_price"] = df[mapping["price"]].apply(safe_float)
        df["free_qty"] = df[mapping["qty"]].apply(parse_qty_generic)
        df["total_qty"] = df["free_qty"]
        df["search_blob"] = (df["article"] + " " + df["name"]).map(contains_text)
        df["search_blob_compact"] = (df["article"] + " " + df["name"]).map(compact_text)
        df["name_tokens"] = df["name"].map(tokenize_text)
        df["sheet_name"] = sheet

        columns = list(raw.columns)
        source_pairs: list[dict[str, str]] = []
        seen_sources: set[str] = set()
        for col in columns:
            col_txt = normalize_text(col)
            m = re.match(r"^(.*?)\s+цена$", col_txt, flags=re.IGNORECASE)
            if not m:
                continue
            source = normalize_text(m.group(1))
            if not source or compact_text(source) in {"НАША"}:
                continue
            qty_col = None
            for candidate in columns:
                candidate_txt = normalize_text(candidate)
                if compact_text(candidate_txt) == compact_text(f"{source} шт"):
                    qty_col = candidate
                    break
            if not qty_col or source in seen_sources:
                continue
            seen_sources.add(source)
            source_pairs.append({"source": source, "price_col": col, "qty_col": qty_col})

        df["source_pairs"] = [source_pairs for _ in range(len(df))]
        df["photo_url"] = ""
        df["photo_name"] = ""
        df["row_codes"] = df.apply(lambda row: build_row_compare_codes(row.get("article", ""), row.get("name", "")), axis=1)
        df["blocked_source_prices"] = [{} for _ in range(len(df))]
        df = df[(df["article_norm"] != "") & (df["name"] != "")].copy()
        df = df.reset_index(drop=True)
        sheets[sheet] = df
    if not sheets:
        raise ValueError("Не удалось прочитать comparison-файл: на листах не найдены обязательные колонки Артикул / Наименование / Наша цена / Наш склад.")

    compatible_df = sheets.get("Совместимые")
    compatible_lookup = build_compatible_price_lookup(compatible_df)
    original_df = sheets.get("Сравнение")
    if compatible_lookup and isinstance(original_df, pd.DataFrame) and not original_df.empty:
        original_df = original_df.copy()
        original_df["blocked_source_prices"] = original_df["row_codes"].apply(lambda codes: merge_blocked_source_prices(codes, compatible_lookup))
        sheets["Сравнение"] = original_df
    return sheets


@st.cache_data(show_spinner=False)
def load_photo_map_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()

    def _sheet_priority(sheet_name: str) -> int:
        name = contains_text(sheet_name)
        if "ФОТО" in name or "СЫЛ" in name:
            return 0
        if "WORKSHEET" in name:
            return 20
        return 10

    def _from_raw(raw: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
        raw = raw.dropna(how="all")
        if raw.empty:
            return pd.DataFrame(columns=["article", "article_norm", "photo_url", "source_sheet", "sheet_priority"])

        raw = raw.copy()
        raw.columns = [normalize_text(c) for c in raw.columns]

        mapping = detect_mapping(raw, PHOTO_COLUMN_ALIASES)

        if not mapping.get("article"):
            for col in raw.columns:
                if compact_text(col) == "АРТИКУЛ":
                    mapping["article"] = col
                    break

        # Для Worksheet сначала стараемся взять чистую колонку images, а не шумную imag.
        if "images" in raw.columns and raw["images"].map(lambda x: bool(extract_first_url(x))).sum() > 0:
            mapping["photo_url"] = "images"
        elif not mapping.get("photo_url") and "imag" in raw.columns and raw["imag"].map(lambda x: bool(extract_first_url(x))).sum() > 0:
            mapping["photo_url"] = "imag"

        if not mapping.get("photo_url") and len(raw.columns) >= 2:
            first_col = mapping.get("article") or raw.columns[0]
            best_col = None
            best_hits = 0
            for col in raw.columns:
                if col == first_col:
                    continue
                hits = raw[col].map(lambda x: bool(extract_first_url(x))).sum()
                if hits > best_hits:
                    best_hits = hits
                    best_col = col
            if best_col is not None and best_hits > 0:
                mapping["photo_url"] = best_col

        if not mapping.get("article") or not mapping.get("photo_url"):
            return pd.DataFrame(columns=["article", "article_norm", "photo_url", "source_sheet", "sheet_priority"])

        out = pd.DataFrame()
        out["article"] = raw[mapping["article"]].map(normalize_text)
        out["article_norm"] = raw[mapping["article"]].map(normalize_article)
        out["photo_url"] = raw[mapping["photo_url"]].map(extract_first_url)
        out["source_sheet"] = sheet_name
        out["sheet_priority"] = _sheet_priority(sheet_name)
        out = out[(out["article_norm"] != "") & (out["photo_url"] != "")].reset_index(drop=True)
        return out

    if suffix == ".csv":
        bio = io.BytesIO(file_bytes)
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
        out = _from_raw(raw, "CSV")
        if out.empty:
            raise ValueError("В файле фото нужны колонки с артикулом и ссылкой на фото.")
        out = out.sort_values(["sheet_priority", "article_norm"]).drop_duplicates(subset=["article_norm"], keep="first").reset_index(drop=True)
        return out[["article", "article_norm", "photo_url", "source_sheet"]]

    sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    parts: list[pd.DataFrame] = []
    for sheet_name, raw in sheets.items():
        part = _from_raw(raw, sheet_name)
        if not part.empty:
            parts.append(part)

    if not parts:
        raise ValueError("В файле фото нужны колонки с артикулом и ссылкой на фото.")

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.sort_values(["sheet_priority", "article_norm"]).drop_duplicates(subset=["article_norm"], keep="first").reset_index(drop=True)
    return combined[["article", "article_norm", "photo_url", "source_sheet"]]


def apply_photo_map(df: pd.DataFrame | None, photo_df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None:
        return None
    out = df.copy()
    if photo_df is None or photo_df.empty:
        out["photo_url"] = out.get("photo_url", "")
        out["photo_name"] = out.get("photo_name", "")
        return out
    lookup = dict(zip(photo_df["article_norm"], photo_df["photo_url"]))
    out["photo_url"] = out["article_norm"].map(lambda x: lookup.get(x, ""))
    out["photo_name"] = out["name"]
    return out


@st.cache_data(show_spinner=False)
def load_avito_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix == ".csv":
        bio = io.BytesIO(file_bytes)
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
        mapping = detect_mapping(raw, AVITO_COLUMN_ALIASES)
        if not mapping.get("title"):
            raise ValueError("Не удалось определить колонку 'Название объявления' в файле Авито.")
        rows = []
        for _, r in raw.iterrows():
            rows.append({
                "ad_id": normalize_text(r[mapping["ad_id"]]) if mapping.get("ad_id") else "",
                "title": normalize_text(r[mapping["title"]]) if mapping.get("title") else "",
                "price": normalize_text(r[mapping["price"]]) if mapping.get("price") else "",
                "url": normalize_text(r[mapping["url"]]) if mapping.get("url") else "",
            })
        out = pd.DataFrame(rows)
        out["title_norm"] = out["title"].map(contains_text)
        return out

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=False)
    ws = wb.active
    headers = [normalize_text(ws.cell(1, c).value) for c in range(1, ws.max_column + 1)]

    def find_header_index(candidates: list[str]) -> Optional[int]:
        for idx, header in enumerate(headers, start=1):
            for cand in candidates:
                if header.lower() == cand.lower():
                    return idx
        for idx, header in enumerate(headers, start=1):
            h = header.lower()
            for cand in candidates:
                c = cand.lower()
                if c in h or h in c:
                    return idx
        return None

    ad_id_col = find_header_index(AVITO_COLUMN_ALIASES["ad_id"])
    title_col = find_header_index(AVITO_COLUMN_ALIASES["title"])
    price_col = find_header_index(AVITO_COLUMN_ALIASES["price"])
    url_col = find_header_index(AVITO_COLUMN_ALIASES["url"])
    if not title_col:
        raise ValueError("Не удалось определить колонку 'Название объявления' в файле Авито.")

    rows = []
    for r in range(2, ws.max_row + 1):
        ad_display, ad_url = cell_display_and_url(ws.cell(r, ad_id_col)) if ad_id_col else ("", "")
        title_display, title_url = cell_display_and_url(ws.cell(r, title_col))
        explicit_url = normalize_text(ws.cell(r, url_col).value) if url_col else ""
        price_value = normalize_text(ws.cell(r, price_col).value) if price_col else ""
        final_url = explicit_url or title_url or ad_url
        if not ad_display and not title_display:
            continue
        rows.append({
            "ad_id": ad_display,
            "title": title_display,
            "price": price_value,
            "url": final_url,
        })
    out = pd.DataFrame(rows)
    out["title_norm"] = out["title"].map(contains_text)
    return out


def init_state() -> None:
    defaults = {
        "comparison_sheets": {},
        "comparison_name": "ещё не загружен",
        "selected_sheet": "Сравнение",
        "current_df": None,
        "photo_df": None,
        "photo_name": "ещё не загружен",
        "avito_df": None,
        "avito_name": "ещё не загружен",
        "search_input": "",
        "submitted_query": "",
        "last_result": None,
        "price_mode": "-12%",
        "custom_discount": 10.0,
        "round100": True,
        "search_mode": "Артикул + коды из названия",
        "template1_footer": DEFAULT_TEMPLATE1_FOOTER,
        "price_patch_input": "",
        "patch_message": "",
        "distributor_threshold": 20.0,
        "distributor_min_qty": 1.0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


def rebuild_current_df() -> None:
    sheets = st.session_state.get("comparison_sheets", {})
    selected = st.session_state.get("selected_sheet", "")
    photo_df = st.session_state.get("photo_df")
    base = sheets.get(selected)
    if isinstance(base, pd.DataFrame):
        st.session_state.current_df = apply_photo_map(base, photo_df)
    else:
        st.session_state.current_df = None


def refresh_all_search_results() -> None:
    sheets = st.session_state.get("comparison_sheets", {})
    photo_df = st.session_state.get("photo_df")
    search_mode = st.session_state.get("search_mode", "Артикул + коды из названия")
    tab_specs = [
        ("Сравнение", "original"),
        ("Уценка", "discount"),
        ("Совместимые", "compatible"),
    ]
    for sheet_name, tab_key in tab_specs:
        base_df = sheets.get(sheet_name) if isinstance(sheets, dict) else None
        if isinstance(base_df, pd.DataFrame):
            sheet_df = apply_photo_map(base_df, photo_df)
        else:
            sheet_df = None
        submitted_key = f"submitted_query_{tab_key}"
        result_key = f"last_result_{tab_key}"
        query = normalize_text(st.session_state.get(submitted_key, ""))
        if query and isinstance(sheet_df, pd.DataFrame):
            st.session_state[result_key] = search_in_df(sheet_df, query, search_mode)
        else:
            st.session_state[result_key] = None


def search_in_df(df: pd.DataFrame, query: str, search_mode: str) -> pd.DataFrame:
    tokens = split_query_parts(query)
    if not tokens:
        return df.iloc[0:0].copy()

    exact_hits = []
    linked_hits = []
    contains_hits = []
    seen: set[str] = set()

    for token in tokens:
        token_norm = normalize_article(token)
        token_upper = contains_text(token)

        exact = df[df["article_norm"] == token_norm]
        for _, row in exact.iterrows():
            key = str(row["article_norm"])
            if key in seen:
                continue
            seen.add(key)
            row_dict = row.to_dict()
            row_dict["match_type"] = "exact"
            row_dict["match_query"] = token
            exact_hits.append(row_dict)

        if search_mode in {"Артикул + коды из названия", "Артикул + название + бренд"} and token_norm:
            linked = df[df["row_codes"].apply(lambda codes: token_norm in (codes or []) if isinstance(codes, list) else False)]
            for _, row in linked.iterrows():
                key = str(row["article_norm"])
                if key in seen:
                    continue
                seen.add(key)
                row_dict = row.to_dict()
                row_dict["match_type"] = "linked"
                row_dict["match_query"] = token
                linked_hits.append(row_dict)

        if search_mode == "Артикул + название + бренд":
            mask = (
                df["search_blob"].str.contains(re.escape(token_upper), na=False, regex=True)
                | df["search_blob_compact"].str.contains(re.escape(token_norm), na=False, regex=True)
            )
            contains = df[mask]
            for _, row in contains.iterrows():
                key = str(row["article_norm"])
                if key in seen:
                    continue
                seen.add(key)
                row_dict = row.to_dict()
                row_dict["match_type"] = "contains"
                row_dict["match_query"] = token
                contains_hits.append(row_dict)

    rows = exact_hits + linked_hits + contains_hits
    if not rows:
        return df.iloc[0:0].copy()
    out = pd.DataFrame(rows)
    rank_map = {"exact": 0, "linked": 1, "contains": 2}
    out["_rank"] = out["match_type"].map(lambda x: rank_map.get(str(x), 99))
    out = out.sort_values(["_rank", "article"]).drop(columns=["_rank"]).reset_index(drop=True)
    return out


def parse_price_updates(text: str) -> list[tuple[str, float]]:
    updates: list[tuple[str, float]] = []
    for line in text.splitlines():
        line = normalize_text(line)
        if not line:
            continue
        cleaned = line.replace("🔽", " ").replace("🔼", " ").replace("—", "-")
        m = re.search(r"([A-Za-zА-Яа-я0-9./_-]+)\s*-?\s*([0-9][0-9\s.,]*)", cleaned)
        if not m:
            continue
        article = normalize_article(m.group(1))
        price_txt = re.sub(r"[^0-9,\.]", "", m.group(2)).replace(",", ".")
        try:
            price = float(price_txt)
        except ValueError:
            continue
        if article:
            updates.append((article, price))
    return updates


def apply_price_updates(df: pd.DataFrame, updates_text: str) -> tuple[pd.DataFrame, str]:
    updates = parse_price_updates(updates_text)
    if not updates:
        return df, "Не нашёл строк для правки цен."
    out = df.copy()
    updated = 0
    missed: list[str] = []
    for article_norm, new_price in updates:
        mask = out["article_norm"] == article_norm
        if mask.any():
            out.loc[mask, "sale_price"] = float(new_price)
            updated += 1
        else:
            missed.append(article_norm)
    msg = f"Обновлено цен: {updated}"
    if missed:
        msg += " | Не найдено: " + ", ".join(missed[:10])
    return out, msg


def get_source_pairs(df: pd.DataFrame) -> list[dict[str, str]]:
    if df is None or df.empty:
        return []
    pairs = df["source_pairs"].iloc[0]
    if isinstance(pairs, list):
        return pairs
    return []


def get_row_offers(row: pd.Series, min_qty: float = 1.0) -> list[dict[str, Any]]:
    offers: list[dict[str, Any]] = []
    hidden_compatible_sources: list[str] = []
    for pair in row.get("source_pairs", []) or []:
        source = pair["source"]
        price = safe_float(row.get(pair["price_col"]), 0.0)
        qty = parse_qty_generic(row.get(pair["qty_col"]))
        if price <= 0 or qty < float(min_qty):
            continue
        if is_blocked_by_compatible_price(row, source, price):
            hidden_compatible_sources.append(f"{source} {fmt_price(price)}")
            continue
        offers.append({
            "source": source,
            "price": price,
            "qty": qty,
            "price_fmt": fmt_price(price),
            "qty_fmt": fmt_qty(qty),
        })

    offers, hidden_outlier_sources = filter_suspicious_low_offers(row, offers)

    try:
        row["hidden_compatible_sources"] = unique_preserve_order(hidden_compatible_sources)
        row["hidden_outlier_sources"] = unique_preserve_order(hidden_outlier_sources)
    except Exception:
        pass

    offers.sort(key=lambda x: (x["price"], -x["qty"], x["source"]))
    return offers


def get_best_offer(row: pd.Series, min_qty: float = 1.0) -> dict[str, Any] | None:
    own_price = safe_float(row.get("sale_price"), 0.0)
    offers = get_row_offers(row, min_qty=min_qty)
    if not offers:
        return None
    best = offers[0]
    best = dict(best)
    if own_price > 0:
        delta = own_price - best["price"]
        best["delta"] = delta
        best["delta_fmt"] = fmt_price(abs(delta))
        best["delta_percent"] = (delta / own_price) * 100.0 if own_price else 0.0
        best["delta_percent_fmt"] = f"{best['delta_percent']:.1f}".replace(".0", "")
        if abs(delta) < 1e-9:
            best["status"] = "цена равна"
        elif delta > 0:
            best["status"] = "лучше нас"
        else:
            best["status"] = "дороже нас"
    else:
        best["status"] = "найдено"
    return best


def build_distributor_compare(result_df: pd.DataFrame, min_qty: float = 1.0) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if result_df is None or result_df.empty:
        return out
    for _, row in result_df.iterrows():
        row_key = str(row.get("article_norm", ""))
        out[row_key] = {
            "row_key": row_key,
            "article": row.get("article", ""),
            "name": row.get("name", ""),
            "sale_price": safe_float(row.get("sale_price"), 0.0),
            "best_offer": get_best_offer(row, min_qty=min_qty),
        }
    return out


def build_all_prices_df(result_df: pd.DataFrame, min_qty: float, price_mode: str, round100: bool, custom_discount: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if result_df is None or result_df.empty:
        return pd.DataFrame()
    for _, row in result_df.iterrows():
        article = str(row.get("article", ""))
        name = str(row.get("name", ""))
        own_price = safe_float(row.get("sale_price"), 0.0)
        own_qty = safe_float(row.get("free_qty"), 0.0)
        selected_price = get_selected_price_raw(row, price_mode, round100, custom_discount)

        rows.append({
            "Артикул": article,
            "Название": name,
            "Источник": "Мы",
            "Цена": own_price,
            "Остаток": own_qty,
            "Наша цена": own_price,
            "Наша цена выбранная": selected_price,
            "Разница к нам, руб": 0.0,
            "Разница к нам, %": 0.0,
            "Статус": "наша позиция",
        })

        for offer in get_row_offers(row, min_qty=min_qty):
            delta = own_price - offer["price"]
            delta_pct = (delta / own_price) * 100.0 if own_price else None
            status = "лучше нас" if delta > 0 else "дороже нас" if delta < 0 else "цена равна"
            rows.append({
                "Артикул": article,
                "Название": name,
                "Источник": offer["source"],
                "Цена": offer["price"],
                "Остаток": offer["qty"],
                "Наша цена": own_price,
                "Наша цена выбранная": selected_price,
                "Разница к нам, руб": delta,
                "Разница к нам, %": delta_pct,
                "Статус": status,
            })
    out = pd.DataFrame(rows)
    out["_is_own"] = out["Источник"].map(lambda x: 0 if str(x) == "Мы" else 1)
    out = out.sort_values(["Артикул", "_is_own", "Цена", "Источник"], ascending=[True, True, True, True], na_position="last").drop(columns=["_is_own"]).reset_index(drop=True)
    return out


def all_prices_to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Все цены")
    bio.seek(0)
    return bio.read()


def build_report_df(df: pd.DataFrame, threshold_percent: float, min_qty: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df is None or df.empty:
        return pd.DataFrame()
    for _, row in df.iterrows():
        own_price = safe_float(row.get("sale_price"), 0.0)
        own_qty = safe_float(row.get("free_qty"), 0.0)
        if own_price <= 0:
            continue
        best = get_best_offer(row, min_qty=min_qty)
        if not best:
            continue
        delta = safe_float(best.get("delta"), 0.0)
        delta_pct = safe_float(best.get("delta_percent"), 0.0)
        if delta <= 0 or delta_pct < float(threshold_percent):
            continue
        rows.append({
            "Артикул": row.get("article", ""),
            "Название": row.get("name", ""),
            "Наш остаток": own_qty,
            "Наша цена": own_price,
            "Лучший дистрибьютер": best["source"],
            "Цена дистрибьютора": best["price"],
            "Остаток дистрибьютора": best["qty"],
            "Разница, руб": delta,
            "Разница, %": round(delta_pct, 2),
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values(["Разница, %", "Разница, руб", "Артикул"], ascending=[False, False, True]).reset_index(drop=True)
    return out


def report_to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Отчёт")
    bio.seek(0)
    return bio.read()


def build_offer_template(df: pd.DataFrame, query: str, round100: bool, footer_text: str, search_mode: str) -> str:
    result_df = search_in_df(df, query, search_mode)
    if result_df.empty:
        return ""
    lines: list[str] = []
    hashtags: list[str] = []
    for _, row in result_df.iterrows():
        if safe_float(row.get("free_qty"), 0.0) > 0:
            avito_raw = safe_float(row.get("sale_price"), 0.0) * (1 - DEFAULT_DISCOUNT_1 / 100)
            cash_raw = avito_raw * 0.90
            avito = round_up_to_100(avito_raw) if round100 else round(avito_raw)
            cash = round_to_nearest_100(cash_raw) if round100 else round(cash_raw)
            lines.append(f"{row['article']} --- {fmt_price(avito)} руб. - Авито / {fmt_price(cash)} руб. за наличный расчет")
        else:
            lines.append(f"{row['article']} --- продан")
        hashtags.append(f"#{normalize_article(row['article'])}")
    footer = [normalize_text(x) for x in str(footer_text).splitlines() if normalize_text(x)]
    if footer:
        lines.extend(footer)
    if hashtags:
        lines.append(",".join(unique_preserve_order(hashtags)))
    return "\n".join(lines)


def build_selected_price_template(df: pd.DataFrame, query: str, price_mode: str, round100: bool, custom_discount: float, search_mode: str) -> str:
    result_df = search_in_df(df, query, search_mode)
    if result_df.empty:
        return ""
    parts = []
    for _, row in result_df.iterrows():
        if safe_float(row.get("free_qty"), 0.0) <= 0:
            continue
        selected_price = get_selected_price_raw(row, price_mode, round100, custom_discount)
        parts.append(f"{normalize_text(row['name'])} --- {fmt_price(selected_price)} руб.")
    return "\n\n".join(parts)


def find_avito_ads(avito_df: pd.DataFrame, result_df: pd.DataFrame) -> pd.DataFrame:
    if avito_df is None or avito_df.empty or result_df is None or result_df.empty:
        return pd.DataFrame()
    tokens = []
    for _, row in result_df.iterrows():
        token = normalize_article(row.get("article", ""))
        if token:
            tokens.append(token)
    tokens = unique_preserve_order(tokens)
    if not tokens:
        return pd.DataFrame()
    matches = []
    for _, row in avito_df.iterrows():
        title_norm = contains_text(row.get("title", ""))
        hit_tokens = [t for t in tokens if t and t in compact_text(title_norm)]
        if hit_tokens:
            item = row.to_dict()
            item["matched_tokens"] = ", ".join(hit_tokens)
            matches.append(item)
    if not matches:
        return pd.DataFrame()
    return pd.DataFrame(matches).drop_duplicates(subset=["title", "url"]).reset_index(drop=True)


def render_sidebar_card_header(title: str, icon: str = "📁", help_text: str = "") -> None:
    tooltip_html = ""
    if normalize_text(help_text):
        tooltip_html = (
            '<div class="sidebar-card-help-wrap">'
            '<div class="sidebar-card-help">?</div>'
            f'<div class="sidebar-card-tooltip">{html.escape(help_text)}</div>'
            '</div>'
        )
    st.markdown(
        f"""
        <div class="sidebar-card-header">
          <div class="sidebar-card-header-main">
            <div class="sidebar-card-icon">{html.escape(icon)}</div>
            <div class="sidebar-card-title-wrap">
              <div class="sidebar-card-kicker">Быстрый доступ</div>
              <div class="sidebar-card-title">{html.escape(title)}</div>
            </div>
          </div>
          {tooltip_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_block_header(title: str, subtitle: str = "", icon: str = "📦", help_text: str = "") -> None:
    tooltip_html = ""
    if normalize_text(help_text):
        tooltip_html = (
            '<div class="block-help-wrap">'
            '<div class="block-help">?</div>'
            f'<div class="block-tooltip">{html.escape(help_text)}</div>'
            '</div>'
        )
    st.markdown(
        f"""
        <div class="block-header">
          <div class="block-header-main">
            <div class="block-icon">{html.escape(icon)}</div>
            <div class="block-title-wrap">
              <div class="block-kicker">Раздел интерфейса</div>
              <div class="section-title">{html.escape(title)}</div>
              <div class="section-sub">{html.escape(subtitle)}</div>
            </div>
          </div>
          <div class="block-header-right">
            <div class="block-sparkles">✦ ✦ ✦</div>
            {tooltip_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_info_banner(title: str, text: str, icon: str = "💡", chips: Optional[list[str]] = None, tone: str = "blue") -> None:
    chips_html = ""
    if chips:
        chips_html = "<div class='banner-chip-row'>" + "".join(
            f"<span class='banner-chip'>{html.escape(chip)}</span>" for chip in chips if normalize_text(chip)
        ) + "</div>"
    st.markdown(
        f"""
        <div class="info-banner tone-{html.escape(tone)}">
          <div class="info-banner-icon">{html.escape(icon)}</div>
          <div class="info-banner-body">
            <div class="info-banner-title">{html.escape(title)}</div>
            <div class="info-banner-text">{html.escape(text)}</div>
            {chips_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def status_visual_class(status: str) -> str:
    status_text = contains_text(status)
    if "ЛУЧШЕ" in status_text:
        return "offer-good"
    if "ДОРОЖЕ" in status_text:
        return "offer-bad"
    if "РАВНА" in status_text:
        return "offer-neutral"
    if "НАША ПОЗИЦИЯ" in status_text:
        return "offer-own"
    return "offer-muted"



def render_results_table(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float, distributor_map: Optional[dict[str, dict[str, Any]]] = None) -> None:
    selected_label = current_price_label(price_mode, custom_discount)
    distributor_map = distributor_map or {}
    rows_html = []

    def state_label_from_class(status_class: str) -> str:
        return {
            "offer-good": "выгоднее",
            "offer-bad": "дороже",
            "offer-neutral": "цена равна",
            "offer-own": "наша позиция",
        }.get(status_class, "найдено")

    for _, row in df.iterrows():
        row_key = str(row.get("article_norm", ""))
        selected_raw = get_selected_price_raw(row, price_mode, round100, custom_discount)
        best = distributor_map.get(row_key, {}).get("best_offer") if row_key in distributor_map else None
        match_type = str(row.get("match_type", ""))

        if match_type == "exact":
            badge_html = "<div class='match-badge match-badge-exact'>Точное совпадение</div>"
        elif match_type == "linked":
            badge_html = "<div class='match-badge match-badge-linked'>Код из названия</div>"
        else:
            badge_html = "<div class='match-badge match-badge-soft'>По названию / бренду</div>"

        if best:
            status_class = status_visual_class(str(best.get("status", "")))
            state_label = state_label_from_class(status_class)
            pct_txt = str(best.get("delta_percent_fmt", "")).strip()
            pct_html = f"-{html.escape(pct_txt)}%" if pct_txt else "—"
            compare_html = f"""
            <div class='best-box {status_class}'>
              <div class='best-top'>
                <span class='best-source-pill'>{html.escape(str(best.get('source', '')))}</span>
                <span class='best-state-pill'>{html.escape(state_label)}</span>
              </div>
              <div class='best-price'>{html.escape(str(best.get('price_fmt', '')))} руб.</div>
              <div class='best-meta-row'>Остаток: <b>{html.escape(str(best.get('qty_fmt', '')))}</b></div>
              <div class='best-delta-row'>Разница к нам: {html.escape(str(best.get('delta_fmt', '')))} руб. • {pct_html}</div>
            </div>
            """
        else:
            compare_html = """
            <div class='best-box best-box-empty'>
              <div class='best-empty-title'>Нет цены лучше</div>
            </div>
            """

        photo_url = normalize_text(row.get("photo_url", ""))
        if photo_url:
            photo_html = f"""
            <a href="{html.escape(photo_url, quote=True)}" target="_blank" class="photo-wrap">
              <img src="{html.escape(photo_url, quote=True)}" class="result-photo" loading="lazy" onerror="this.style.display='none'; this.parentNode.innerHTML='<div class=&quot;photo-empty photo-empty-small&quot;>нет фото</div>';">
            </a>
            """
        else:
            photo_html = "<div class='photo-empty photo-empty-small'>нет фото</div>"

        rows_html.append(
            f"""
            <tr>
              <td class='item-col'>
                <div class='item-wrap'>
                  <div class='item-photo'>{photo_html}</div>
                  <div class='item-main'>
                    <div class='item-top'><span class='article-pill'>{html.escape(str(row['article']))}</span></div>
                    <div class='name-cell'>{html.escape(str(row['name']))}</div>
                    {badge_html}
                  </div>
                </div>
              </td>
              <td>{fmt_qty(row['free_qty'])}</td>
              <td class='sale-col'>{fmt_price(row['sale_price'])} руб.</td>
              <td class='selected-col'>{fmt_price(selected_raw)} руб.</td>
              <td class='compare-col'>{compare_html}</td>
            </tr>
            """
        )

    table_html = f"""
    <!doctype html>
    <html><head><meta charset='utf-8'/>
    <style>
      body {{ margin:0; font-family: Inter, Arial, sans-serif; background: transparent; }}
      .wrap {{ background:linear-gradient(180deg, #ffffff 0%, #fbfdff 100%); border:1px solid #dbe5f1; border-radius:22px; overflow:hidden; box-shadow: 0 10px 26px rgba(15,23,42,.06); }}
      table {{ width:100%; border-collapse:separate; border-spacing:0; font-size:14px; }}
      thead th {{ position: sticky; top: 0; z-index: 2; background:linear-gradient(180deg, #f4f8ff 0%, #eef3fb 100%); color:#334155; text-align:left; padding:12px 12px; font-weight:800; border-bottom:1px solid #d7e1ef; }}
      tbody td {{ padding:12px; border-bottom:1px solid #e5edf6; vertical-align:top; color:#1e293b; background: rgba(255,255,255,.96); }}
      tbody tr:nth-child(even) td {{ background: #fcfdff; }}
      tbody tr:hover td {{ background: #f7faff; }}
      .article-pill {{ display:inline-block; padding:6px 10px; border-radius:999px; background:#edf2ff; color:#315efb; font-weight:800; white-space:nowrap; }}
      .name-cell {{ font-weight:800; line-height:1.33; color:#1e293b; margin-bottom:6px; max-width: 560px; }}
      .match-badge {{ display:inline-block; padding:4px 9px; border-radius:999px; font-size:12px; font-weight:800; }}
      .match-badge-exact {{ background:#e8f7ee; color:#15803d; }}
      .match-badge-linked {{ background:#e8f1ff; color:#1d4ed8; }}
      .match-badge-soft {{ background:#fff4e5; color:#b45309; }}
      .sale-col {{ font-weight:800; white-space:nowrap; }}
      .selected-col {{ background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%); border-left:1px solid #c7d7ff; border-right:1px solid #c7d7ff; font-weight:900; color:#315efb; white-space:nowrap; }}
      .compare-col {{ min-width:230px; }}
      .best-box {{ border-radius:18px; padding:11px 12px; min-width:190px; border:1px solid #dce6f7; background:linear-gradient(180deg, #f8fbff 0%, #f3f8ff 100%); box-shadow: inset 0 1px 0 rgba(255,255,255,.72); }}
      .best-box-empty {{ text-align:center; background:linear-gradient(180deg, #fafcff 0%, #f5f7fb 100%); border-color:#e2e8f0; min-height:76px; display:flex; align-items:center; justify-content:center; }}
      .best-empty-title {{ color:#64748b; font-weight:800; }}
      .best-top {{ display:flex; justify-content:space-between; gap:8px; align-items:center; margin-bottom:7px; }}
      .best-source-pill, .best-state-pill {{ display:inline-flex; align-items:center; padding:5px 10px; border-radius:999px; font-size:12px; font-weight:800; line-height:1; }}
      .best-price {{ font-size:18px; font-weight:900; color:#12348a; line-height:1.15; margin-bottom:6px; }}
      .best-meta-row {{ font-size:12px; color:#475569; margin-bottom:5px; }}
      .best-delta-row {{ font-size:12px; color:#64748b; line-height:1.45; }}
      .offer-good {{ border-color:#cfead6; background:linear-gradient(180deg, #fbfffc 0%, #f2fff6 100%); }}
      .offer-good .best-source-pill {{ background:#e9efff; color:#315efb; }}
      .offer-good .best-state-pill {{ background:#e8f7ee; color:#15803d; }}
      .offer-good .best-price {{ color:#103a8c; }}
      .offer-bad {{ border-color:#f7d7dd; background:linear-gradient(180deg, #fffafb 0%, #fff3f4 100%); }}
      .offer-bad .best-source-pill {{ background:#ffe8ec; color:#be123c; }}
      .offer-bad .best-state-pill {{ background:#ffe8ec; color:#be123c; }}
      .offer-bad .best-price {{ color:#991b1b; }}
      .offer-neutral {{ border-color:#d7e2ff; background:linear-gradient(180deg, #fbfdff 0%, #f3f7ff 100%); }}
      .offer-neutral .best-source-pill {{ background:#e9efff; color:#315efb; }}
      .offer-neutral .best-state-pill {{ background:#eef4ff; color:#315efb; }}
      .offer-own .best-source-pill {{ background:#eef2ff; color:#315efb; }}
      .offer-own .best-state-pill {{ background:#f1f5f9; color:#475569; }}
      .photo-col {{ width:92px; text-align:center; }}
      .photo-wrap {{ display:inline-flex; align-items:center; justify-content:center; width:72px; height:72px; border-radius:14px; overflow:hidden; border:1px solid #dbe5f1; background:#f8fbff; text-decoration:none; }}
      .result-photo {{ width:100%; height:100%; object-fit:cover; display:block; }}
      .photo-empty {{ border-radius:14px; display:flex; align-items:center; justify-content:center; background:#f8fafc; border:1px dashed #d6deea; color:#94a3b8; font-size:11px; font-weight:800; text-transform:uppercase; }}
      .photo-empty-small {{ width:72px; height:72px; }}
    </style></head><body>
      <div class='wrap'><table>
        <thead><tr><th>Товар</th><th>Наш склад</th><th>Наша цена</th><th>{html.escape(selected_label)}</th><th>Где лучше нас</th></tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table></div>
    </body></html>
    """
    height = min(max(220, 72 + len(df) * 72), 1050)
    components.html(table_html, height=height, scrolling=True)


def render_all_prices_block(result_df: pd.DataFrame, min_qty: float, price_mode: str, round100: bool, custom_discount: float) -> None:
    all_prices_df = build_all_prices_df(result_df, min_qty, price_mode, round100, custom_discount)
    if all_prices_df.empty:
        st.info("Для текущего результата нет данных по всем ценам.")
        return
    for article, group_df in all_prices_df.groupby("Артикул", sort=False):
        base_name = normalize_text(group_df.iloc[0].get("Название", ""))
        own_row = group_df[group_df["Источник"] == "Мы"].head(1)
        own_price_line = ""
        if not own_row.empty:
            own_price_line = f"Наша цена: {fmt_price(own_row.iloc[0]['Цена'])} руб. • Остаток: {fmt_qty(own_row.iloc[0]['Остаток'])}"
        st.markdown(
            f"""
            <div class='all-prices-head'>
              <div>
                <div class='all-prices-article'>{html.escape(article)}</div>
                <div class='all-prices-name'>{html.escape(base_name)}</div>
                <div class='all-prices-own'>{own_price_line}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        cols = st.columns(max(len(group_df), 1))
        for i, (_, rec) in enumerate(group_df.iterrows()):
            with cols[i]:
                source = str(rec.get("Источник", ""))
                status = str(rec.get("Статус", ""))
                status_class = status_visual_class(status)
                badge = {
                    "offer-good": "🟢 выгоднее",
                    "offer-bad": "🔴 дороже",
                    "offer-neutral": "🟡 цена равна",
                    "offer-own": "🔵 наша позиция",
                    "offer-muted": "⚪ найдено",
                }.get(status_class, status)
                st.markdown(
                    "<div class='offer-card-simple'>"
                    f"<div class='offer-card-source'>{html.escape(source)}</div>"
                    f"<span class='offer-status-badge {status_class}'>{html.escape(badge)}</span>"
                    f"<div class='offer-card-price'>{html.escape(fmt_price(rec.get('Цена')))} руб.</div>"
                    f"<div class='offer-card-meta'>Остаток: <b>{html.escape(fmt_qty(rec.get('Остаток')))}</b></div>"
                    + (
                        f"<div class='offer-card-meta'>Разница к нам: {html.escape(fmt_price(rec.get('Разница к нам, руб')))} руб. • "
                        f"{html.escape(str(round(float(rec.get('Разница к нам, %')), 2)).replace('.0', ''))}%</div>"
                        if source != "Мы" and pd.notna(rec.get("Разница к нам, %"))
                        else ""
                    )
                    + "</div>",
                    unsafe_allow_html=True,
                )
        with st.expander(f"Таблица по {article}"):
            show = group_df.copy()
            for col in ["Цена", "Остаток", "Наша цена", "Наша цена выбранная", "Разница к нам, руб"]:
                if col in show.columns:
                    show[col] = show[col].apply(lambda v: fmt_price(v) if "цена" in col.lower() or "руб" in col.lower() else fmt_qty(v))
            show["Разница к нам, %"] = show["Разница к нам, %"].apply(lambda v: (str(round(float(v), 2)).replace(".0", "") + "%") if pd.notna(v) else "")
            st.dataframe(show, use_container_width=True, hide_index=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    st.download_button(
        "⬇️ Скачать все цены в Excel",
        all_prices_to_excel_bytes(all_prices_df),
        file_name="moy_tovar_all_prices.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


def render_results_insight_dashboard(result_df: pd.DataFrame, compare_map: dict[str, dict[str, Any]], source_pairs: list[dict[str, str]]) -> None:
    found_count = len(result_df) if isinstance(result_df, pd.DataFrame) else 0
    better_rows = 0
    gains: list[float] = []
    for item in compare_map.values():
        offer = item.get("best_offer")
        if offer and safe_float(offer.get("delta"), 0.0) > 0:
            better_rows += 1
            gains.append(safe_float(offer.get("delta_percent"), 0.0))
    avg_gain = sum(gains) / len(gains) if gains else 0.0
    cards = [
        ("🔎", "Найдено позиций", str(found_count), "Сколько строк вошло в текущий поиск"),
        ("💚", "Есть цена лучше", str(better_rows), "Сколько позиций реально дешевле у поставщиков"),
        ("📈", "Средняя выгода", (f"{avg_gain:.1f}%" if gains else "—"), "Считается приложением, не берётся из готовых колонок Excel"),
        ("🧩", "Источников найдено", str(len(source_pairs)), ", ".join([x["source"] for x in source_pairs]) if source_pairs else "Нет колонок источников"),
    ]
    html_cards = "".join(
        f"<div class='insight-card'><div class='insight-top'><span class='insight-icon'>{icon}</span><span class='insight-label'>{label}</span></div><div class='insight-value'>{value}</div><div class='insight-note'>{note}</div></div>"
        for icon, label, value, note in cards
    )
    st.markdown(f"<div class='insight-grid'>{html_cards}</div>", unsafe_allow_html=True)


def render_avito_block(avito_df: pd.DataFrame, result_df: pd.DataFrame) -> None:
    ads = find_avito_ads(avito_df, result_df)
    if ads.empty:
        st.caption("Объявления Авито по этим артикулам не найдены.")
        return
    st.caption(f"Найдено объявлений Авито: {len(ads)}")
    for _, row in ads.head(20).iterrows():
        title = normalize_text(row.get("title", ""))
        url = normalize_text(row.get("url", ""))
        st.markdown(f"**{html.escape(title)}**", unsafe_allow_html=True)
        meta = []
        if normalize_text(row.get("ad_id", "")):
            meta.append(f"ID: {normalize_text(row.get('ad_id'))}")
        if normalize_text(row.get("price", "")):
            meta.append(f"Цена: {normalize_text(row.get('price'))}")
        if normalize_text(row.get("matched_tokens", "")):
            meta.append(f"Совпадения: {normalize_text(row.get('matched_tokens'))}")
        if meta:
            st.caption(" • ".join(meta))
        if url:
            st.link_button("Открыть объявление", url, use_container_width=False)
        st.markdown("---")


def to_excel_bytes(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float, min_qty: float) -> bytes:
    export_df = df.copy()
    export_df[current_price_label(price_mode, custom_discount)] = export_df.apply(lambda row: fmt_price(get_selected_price_raw(row, price_mode, round100, custom_discount)), axis=1)
    export_df["Лучшая цена поставщика"] = export_df.apply(lambda row: (get_best_offer(row, min_qty=min_qty) or {}).get("price_fmt", ""), axis=1)
    export_df["Лучший поставщик"] = export_df.apply(lambda row: (get_best_offer(row, min_qty=min_qty) or {}).get("source", ""), axis=1)
    export_df["Фото"] = export_df.get("photo_url", "")
    export_df = export_df[["article", "name", "free_qty", "sale_price", current_price_label(price_mode, custom_discount), "Лучший поставщик", "Лучшая цена поставщика", "Фото"]].rename(columns={
        "article": "Артикул",
        "name": "Название",
        "free_qty": "Наш склад",
        "sale_price": "Наша цена",
    })
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Результаты")
    bio.seek(0)
    return bio.read()


st.markdown(
    """
    <style>
    .stApp { background: #eef3f9; }
    header[data-testid="stHeader"] { background: rgba(0,0,0,0); }
    [data-testid="stDecoration"] { display: none; }
    .block-container { max-width: 1560px; padding-top: 3.4rem; padding-bottom: 1.2rem; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #08122f 0%, #102358 55%, #172a63 100%); border-right: 1px solid rgba(255,255,255,.08); }
    [data-testid="stSidebar"] * { color: #e9efff !important; }
    .sidebar-brand { display:flex; align-items:center; gap:12px; margin: 0.15rem 0 0.95rem 0; padding: 0.15rem 0.1rem 0.35rem 0.1rem; }
    .sidebar-brand-logo { width:44px; height:44px; border-radius:14px; background: linear-gradient(180deg, rgba(255,255,255,.18), rgba(255,255,255,.08)); display:flex; align-items:center; justify-content:center; box-shadow: inset 0 1px 0 rgba(255,255,255,.15); font-size:22px; }
    .sidebar-brand-title { font-size: 1.22rem; font-weight: 900; line-height:1.05; color:#ffffff !important; }
    .sidebar-brand-sub { font-size: .82rem; color: #c7d6ff !important; margin-top: 4px; }
    .sidebar-card { background: linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.045)); border: 1px solid rgba(255,255,255,.13); border-radius: 22px; padding: 1rem 0.95rem 0.95rem 0.95rem; margin: 0.95rem 0 1.05rem 0; box-shadow: 0 12px 26px rgba(2, 8, 23, .24), inset 0 1px 0 rgba(255,255,255,.06); position: relative; overflow: hidden; }
    .sidebar-card::before { content: ''; position: absolute; inset: 0 auto auto 0; width: 100%; height: 3px; background: linear-gradient(90deg, rgba(111,163,255,.95) 0%, rgba(49,94,251,.95) 100%); opacity: .95; }
    .sidebar-card-header { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; margin-bottom: .6rem; padding-bottom: .55rem; border-bottom: 1px solid rgba(255,255,255,.10); }
    .sidebar-card-header-main { display:flex; align-items:center; gap:10px; min-width:0; }
    .sidebar-card-title-wrap { min-width: 0; }
    .sidebar-card-kicker { color:#cfe0ff !important; font-size:10px; text-transform: uppercase; letter-spacing:.06em; font-weight:900; margin-bottom:2px; }
    .sidebar-card-icon { width:34px; height:34px; border-radius:12px; background: linear-gradient(180deg, rgba(255,255,255,.18), rgba(255,255,255,.08)); display:flex; align-items:center; justify-content:center; font-size:17px; box-shadow: inset 0 1px 0 rgba(255,255,255,.12); flex: 0 0 34px; }
    .sidebar-card-title { font-size: 1.01rem; font-weight: 900; color:#ffffff !important; line-height:1.15; margin:0; }
    .sidebar-card-help-wrap { position: relative; flex: 0 0 auto; }
    .sidebar-card-help { display:flex; align-items:center; justify-content:center; width:24px; height:24px; border-radius:999px; border:1px solid rgba(255,255,255,.18); background: rgba(255,255,255,.08); color:#ffffff !important; font-size:12px; font-weight:900; cursor:help; user-select:none; }
    .sidebar-card-tooltip { position:absolute; right:0; top:30px; width:250px; max-width:min(250px, 66vw); padding:10px 11px; border-radius:12px; background:#f8fbff; color:#0f172a !important; font-size:12px; line-height:1.45; box-shadow:0 16px 34px rgba(2, 8, 23, .30); opacity:0; transform:translateY(6px); pointer-events:none; transition:opacity .18s ease, transform .18s ease; z-index:35; }
    .sidebar-card-tooltip::before { content:''; position:absolute; top:-6px; right:9px; width:12px; height:12px; background:#f8fbff; transform:rotate(45deg); }
    .sidebar-card-help-wrap:hover .sidebar-card-tooltip { opacity:1; transform:translateY(0); }
    .sidebar-card-note { font-size: .79rem; line-height: 1.52; color:#c7d6ff !important; margin-bottom: .65rem; }
    .sidebar-status { background: rgba(7, 31, 74, .92); border: 1px solid rgba(255,255,255,.06); border-radius: 14px; padding: .76rem .82rem; color:#ffffff !important; font-weight: 800; margin-top: .58rem; }
    .sidebar-mini { font-size:.78rem; color:#c7d6ff !important; line-height:1.5; margin-top:.65rem; }
    [data-testid="stSidebar"] .stFileUploader section,
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"],
    [data-testid="stSidebar"] section[data-testid="stFileUploaderDropzone"] {
        background: linear-gradient(180deg, rgba(10,24,67,.92), rgba(9,20,56,.88)) !important;
        border: 1px dashed rgba(140,173,255,.34) !important;
        border-radius: 18px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.05) !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
        color: #dfe8ff !important;
        -webkit-text-fill-color: #dfe8ff !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button,
    [data-testid="stSidebar"] .stFileUploader button,
    [data-testid="stSidebar"] .stFileUploader [data-baseweb="button"] {
        background: linear-gradient(180deg, #3b6dff 0%, #2758ef 100%) !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        border: none !important;
        border-radius: 14px !important;
        font-weight: 800 !important;
        box-shadow: 0 10px 20px rgba(49,94,251,.28) !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderFileData"],
    [data-testid="stSidebar"] [data-testid="stFileUploaderFile"],
    [data-testid="stSidebar"] [data-testid="stFileUploaderFileName"] {
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stTextArea textarea,
    [data-testid="stSidebar"] .stTextInput input,
    [data-testid="stSidebar"] .stNumberInput input,
    [data-testid="stSidebar"] [data-baseweb="input"] input,
    [data-testid="stSidebar"] [data-baseweb="base-input"] input,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea,
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: linear-gradient(180deg, rgba(9,22,60,.98), rgba(7,18,50,.96)) !important;
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        caret-color: #ffffff !important;
        border: none !important;
        border-radius: 16px !important;
        box-shadow: inset 0 0 0 1px rgba(135,170,255,.24), 0 8px 18px rgba(2,8,23,.12) !important;
    }
    [data-testid="stSidebar"] .stTextArea textarea::placeholder,
    [data-testid="stSidebar"] .stTextInput input::placeholder,
    [data-testid="stSidebar"] .stNumberInput input::placeholder,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea::placeholder {
        color: #9fb4ef !important;
        -webkit-text-fill-color: #9fb4ef !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stNumberInput button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="button"],
    [data-testid="stSidebar"] .stNumberInput svg {
        background: rgba(255,255,255,.04) !important;
        color: #cfe0ff !important;
        fill: #cfe0ff !important;
        stroke: #cfe0ff !important;
        border-color: rgba(135,170,255,.18) !important;
        opacity: 1 !important;
    }

    /* stronger sidebar field styling to override white baseweb wrappers */
    [data-testid="stSidebar"] .stNumberInput,
    [data-testid="stSidebar"] .stTextInput,
    [data-testid="stSidebar"] .stTextArea,
    [data-testid="stSidebar"] .stSelectbox {
        background: transparent !important;
    }
    [data-testid="stSidebar"] .stNumberInput > div,
    [data-testid="stSidebar"] .stTextInput > div,
    [data-testid="stSidebar"] .stTextArea > div,
    [data-testid="stSidebar"] .stSelectbox > div {
        background: transparent !important;
    }
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"],
    [data-testid="stSidebar"] .stTextInput [data-baseweb="base-input"],
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"],
    [data-testid="stSidebar"] .stTextInput [data-baseweb="input"],
    [data-testid="stSidebar"] .stTextArea [data-baseweb="textarea"],
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] {
        background: linear-gradient(180deg, rgba(9,22,60,.98), rgba(7,18,50,.96)) !important;
        border-radius: 16px !important;
        box-shadow: inset 0 0 0 1px rgba(135,170,255,.24), 0 8px 18px rgba(2,8,23,.12) !important;
        border: none !important;
    }
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] > div,
    [data-testid="stSidebar"] .stTextInput [data-baseweb="base-input"] > div,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] > div,
    [data-testid="stSidebar"] .stTextInput [data-baseweb="input"] > div,
    [data-testid="stSidebar"] .stTextArea [data-baseweb="textarea"] > div,
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
        background: transparent !important;
        border-radius: 16px !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] input,
    [data-testid="stSidebar"] .stTextInput [data-baseweb="base-input"] input,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] input,
    [data-testid="stSidebar"] .stTextInput [data-baseweb="input"] input,
    [data-testid="stSidebar"] .stTextArea [data-baseweb="textarea"] textarea,
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] input,
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] div[role="combobox"] {
        background: transparent !important;
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] > div > button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] > div > button {
        background: rgba(255,255,255,.04) !important;
        color: #cfe0ff !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stTextArea [data-baseweb="textarea"] textarea {
        min-height: 110px;
    }
    /* final hard override for white sidebar fields */
    [data-testid="stSidebar"] input:not([type="checkbox"]):not([type="radio"]),
    [data-testid="stSidebar"] textarea,
    [data-testid="stSidebar"] select,
    [data-testid="stSidebar"] div[role="combobox"],
    [data-testid="stSidebar"] [data-baseweb="input"],
    [data-testid="stSidebar"] [data-baseweb="base-input"],
    [data-testid="stSidebar"] [data-baseweb="textarea"],
    [data-testid="stSidebar"] [data-baseweb="select"],
    [data-testid="stSidebar"] [data-baseweb="input"] > div,
    [data-testid="stSidebar"] [data-baseweb="base-input"] > div,
    [data-testid="stSidebar"] [data-baseweb="textarea"] > div,
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: linear-gradient(180deg, rgba(9,22,60,.98), rgba(7,18,50,.96)) !important;
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        border-color: rgba(135,170,255,.24) !important;
        border: none !important;
        box-shadow: inset 0 0 0 1px rgba(135,170,255,.24), 0 8px 18px rgba(2,8,23,.12) !important;
        border-radius: 16px !important;
    }
    [data-testid="stSidebar"] input::placeholder,
    [data-testid="stSidebar"] textarea::placeholder {
        color: #9fb4ef !important;
        -webkit-text-fill-color: #9fb4ef !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] svg,
    [data-testid="stSidebar"] div[role="combobox"] svg {
        fill: #cfe0ff !important;
        color: #cfe0ff !important;
    }
    [data-testid="stSidebar"] [data-baseweb="input"] button,
    [data-testid="stSidebar"] [data-baseweb="base-input"] button,
    [data-testid="stSidebar"] .stNumberInput button {
        background: rgba(255,255,255,.05) !important;
        color: #cfe0ff !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stTextArea textarea,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea {
        padding: 12px 14px !important;
        line-height: 1.55 !important;
    }
    [data-testid="stSidebar"] .stButton > button,
    [data-testid="stSidebar"] .stDownloadButton > button {
        width: 100% !important;
        min-height: 48px !important;
        background: linear-gradient(180deg, #3b6dff 0%, #2758ef 100%) !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        border: none !important;
        border-radius: 16px !important;
        font-weight: 900 !important;
        box-shadow: 0 10px 22px rgba(49,94,251,.30) !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover,
    [data-testid="stSidebar"] .stDownloadButton > button:hover {
        background: linear-gradient(180deg, #4a79ff 0%, #2d61f2 100%) !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }
    [data-testid="stSidebar"] .stButton > button:disabled,
    [data-testid="stSidebar"] .stDownloadButton > button:disabled {
        background: linear-gradient(180deg, rgba(96,114,167,.72), rgba(80,95,143,.72)) !important;
        color: rgba(255,255,255,.86) !important;
        -webkit-text-fill-color: rgba(255,255,255,.86) !important;
        box-shadow: none !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stRadio p,
    [data-testid="stSidebar"] .stCheckbox p,
    [data-testid="stSidebar"] .stSelectbox p {
        color: #eef4ff !important;
        -webkit-text-fill-color: #eef4ff !important;
    }
    .topbar { position: relative; background: linear-gradient(110deg, #0f172a 0%, #1742a8 56%, #2d6bff 100%); color: white; padding: 18px 20px; border-radius: 24px; margin-top: 0.55rem; margin-bottom: 14px; box-shadow: 0 18px 38px rgba(15, 23, 42, .22); overflow: hidden; }
    .topbar-grid { display:grid; grid-template-columns: 1.6fr 1fr 1fr 1fr; gap: 12px; align-items:center; position:relative; z-index:1; }
    .brand-box { display:flex; gap:14px; align-items:center; }
    .logo { width:58px;height:58px;border-radius:18px;background:rgba(255,255,255,.16); display:flex;align-items:center;justify-content:center;font-size:28px;font-weight:700; }
    .brand-title { font-size: 25px; font-weight: 900; line-height: 1; letter-spacing: -.02em; }
    .brand-sub { font-size: 13px; opacity: .92; margin-top: 6px; }
    .stat-box { background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.14); border-radius: 18px; padding: 12px 13px; min-height: 76px; backdrop-filter: blur(3px); }
    .stat-cap { font-size: 12px; opacity: .82; margin-bottom: 6px; }
    .stat-val { font-size: 16px; font-weight: 800; line-height: 1.3; }
    .toolbar, .result-wrap { position: relative; background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%); border: 1px solid #dbe5f1; border-radius: 22px; padding: 16px 18px 18px 18px; margin-bottom: 14px; box-shadow: 0 10px 26px rgba(15, 23, 42, .06); overflow: hidden; }
    .block-header { display:flex; align-items:flex-start; justify-content:space-between; gap:16px; padding: 2px 0 14px 0; margin-bottom: 14px; border-bottom: 1px solid #e7eef9; position: relative; }
    .block-header-main { display:flex; align-items:flex-start; gap:14px; min-width: 0; }
    .block-header-right { display:flex; align-items:flex-start; gap:10px; flex: 0 0 auto; }
    .block-icon { width: 48px; height: 48px; border-radius: 16px; background: linear-gradient(180deg, #3767ff 0%, #2455ef 100%); color: #ffffff; display:flex; align-items:center; justify-content:center; font-size: 24px; flex: 0 0 48px; }
    .block-title-wrap { min-width: 0; }
    .block-kicker { display:inline-flex; align-items:center; padding: 4px 9px; margin-bottom: 7px; border-radius: 999px; background: #eef4ff; border: 1px solid #d8e5ff; color: #315efb; font-size: 11px; font-weight: 900; letter-spacing: .04em; text-transform: uppercase; }
    .section-title { font-size: 22px; font-weight: 900; color:#0f172a; margin:0 0 5px 0; line-height:1.12; letter-spacing:-0.02em; }
    .section-sub { font-size: 13px; color:#64748b; margin:0; line-height:1.55; max-width: 980px; }
    .block-sparkles { display:flex; align-items:center; gap: 3px; color:#89a9ff; font-size: 12px; font-weight: 900; letter-spacing: .04em; opacity: .9; margin-top: 5px; }
    .block-help-wrap { position: relative; flex: 0 0 auto; }
    .block-help { display:flex; align-items:center; justify-content:center; width: 32px; height: 32px; border-radius: 999px; border: 1px solid #cfe0ff; background: linear-gradient(180deg, #f6f9ff 0%, #eef4ff 100%); color: #315efb; font-size: 15px; font-weight: 900; cursor: help; user-select: none; }
    .block-tooltip { position: absolute; right: 0; top: 40px; width: 340px; max-width: min(340px, 82vw); padding: 13px 14px; border-radius: 16px; background: #0f172a; color: #f8fbff; font-size: 12.8px; line-height: 1.5; box-shadow: 0 18px 36px rgba(15, 23, 42, .28); opacity: 0; transform: translateY(6px); pointer-events: none; transition: opacity .18s ease, transform .18s ease; z-index: 20; }
    .block-help-wrap:hover .block-tooltip { opacity: 1; transform: translateY(0); }
    .info-banner { display:flex; gap:14px; align-items:flex-start; padding:15px 16px; margin: 6px 0 14px 0; border-radius: 18px; border: 1px solid #dbe7fb; background: linear-gradient(180deg, #fbfdff 0%, #f5f9ff 100%); box-shadow: 0 8px 18px rgba(15,23,42,.05); }
    .info-banner-icon { width:42px; height:42px; flex:0 0 42px; border-radius: 14px; display:flex; align-items:center; justify-content:center; font-size: 20px; background: linear-gradient(180deg, #3767ff 0%, #2455ef 100%); color:#fff; }
    .info-banner-title { font-size: 15px; font-weight: 900; color:#0f172a; margin-bottom: 4px; }
    .info-banner-text { font-size: 13px; line-height: 1.55; color:#64748b; }
    .banner-chip-row { display:flex; flex-wrap:wrap; gap:8px; margin-top: 10px; }
    .banner-chip { display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 999px; background:#eef4ff; border:1px solid #d8e5ff; color:#315efb; font-size: 12px; font-weight: 800; }
    .tone-green { background: linear-gradient(180deg, #fbfffd 0%, #f2fff7 100%); border-color: #d2f1dd; }
    .tone-purple { background: linear-gradient(180deg, #fcfbff 0%, #f6f3ff 100%); border-color: #e6dcff; }
    .insight-grid { display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 14px 0 16px 0; }
    .insight-card { background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%); border: 1px solid #dbe7fb; border-radius: 20px; padding: 14px 15px; box-shadow: 0 8px 18px rgba(15,23,42,.05); }
    .insight-top { display:flex; align-items:center; gap:8px; margin-bottom: 10px; }
    .insight-icon { width:32px; height:32px; display:flex; align-items:center; justify-content:center; border-radius: 12px; background:#eef4ff; font-size:16px; }
    .insight-label { color:#64748b; font-size:12px; font-weight:800; }
    .insight-value { color:#0f172a; font-size: 28px; font-weight: 900; line-height:1.1; margin-bottom: 6px; }
    .insight-note { color:#6b7c93; font-size:12px; line-height:1.45; }
    .all-prices-head { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; margin: 14px 0 10px 0; padding: 14px 16px; border-radius: 18px; background: linear-gradient(180deg, #fbfdff 0%, #f5f9ff 100%); border:1px solid #dbe7fb; }
    .all-prices-article { color:#315efb; font-size: 18px; font-weight: 900; margin-bottom: 4px; }
    .all-prices-name { color:#0f172a; font-size: 14px; font-weight: 800; line-height: 1.45; }
    .all-prices-own { margin-top: 6px; color:#64748b; font-size: 12.5px; }
    .offer-card-simple { border-radius: 18px; padding: 14px; border:1px solid #dbe7fb; background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%); min-height: 140px; box-shadow: 0 8px 18px rgba(15,23,42,.05); }
    .offer-card-source { color:#0f172a; font-size: 15px; font-weight: 900; margin-bottom: 10px; }
    .offer-status-badge { display:inline-flex; align-items:center; justify-content:center; padding:5px 9px; border-radius:999px; font-size:11px; font-weight:900; margin-bottom: 8px; }
    .offer-good { background:#e9f9ef; color:#15803d; }
    .offer-bad { background:#fff1f2; color:#be123c; }
    .offer-neutral { background:#eef4ff; color:#315efb; }
    .offer-own { background:#f3f4f6; color:#475569; }
    .offer-muted { background:#f8fafc; color:#64748b; }
    .offer-card-price { color:#0f2f83; font-size: 24px; font-weight: 900; line-height: 1.15; margin-bottom: 6px; }
    .offer-card-meta { color:#64748b; font-size: 12.5px; line-height:1.45; margin-bottom: 4px; }
    </style>
    """,
    unsafe_allow_html=True,
)


with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-brand">
          <div class="sidebar-brand-logo">📦</div>
          <div>
            <div class="sidebar-brand-title">Мой Товар</div>
            <div class="sidebar-brand-sub">comparison-файл + фото + поиск 💙</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Comparison-файл", "📘", "Главный файл приложения. Содержит листы Сравнение, Уценка, Совместимые.")
    uploaded = st.file_uploader("Загрузить comparison-файл", type=["xlsx", "xlsm"], label_visibility="collapsed")
    if uploaded is not None:
        try:
            st.session_state.comparison_sheets = load_comparison_workbook(uploaded.name, uploaded.getvalue())
            st.session_state.comparison_name = uploaded.name
            available = list(st.session_state.comparison_sheets.keys())
            if available and st.session_state.selected_sheet not in available:
                st.session_state.selected_sheet = available[0]
            rebuild_current_df()
            refresh_all_search_results()
        except Exception as exc:
            st.error(f"Ошибка файла: {exc}")
    st.markdown(f'<div class="sidebar-status">Файл: {html.escape(st.session_state.get("comparison_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-mini">Листы теперь открываются сверху как 3 отдельные вкладки: <b>Оригинал</b>, <b>Уценка</b>, <b>Совместимые</b>.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Фото товаров", "🖼️", "Файл с артикулами и ссылками на фото. Картинка показывается прямо в результатах поиска.")
    photo_uploaded = st.file_uploader("Загрузить файл фото", type=["xlsx", "xls", "xlsm", "csv"], key="photo_uploader", label_visibility="collapsed")
    if photo_uploaded is not None:
        try:
            st.session_state.photo_df = load_photo_map_file(photo_uploaded.name, photo_uploaded.getvalue())
            st.session_state.photo_name = photo_uploaded.name
            rebuild_current_df()
            refresh_all_search_results()
        except Exception as exc:
            st.error(f"Ошибка файла фото: {exc}")
    st.markdown(f'<div class="sidebar-status">Фото: {html.escape(st.session_state.get("photo_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Авито", "🛒", "Необязательный файл. Помогает быстро найти действующие объявления по найденным артикулам.")
    avito_uploaded = st.file_uploader("Загрузить файл Авито", type=["xlsx", "xlsm", "csv"], key="avito_uploader", label_visibility="collapsed")
    if avito_uploaded is not None:
        try:
            st.session_state.avito_df = load_avito_file(avito_uploaded.name, avito_uploaded.getvalue())
            st.session_state.avito_name = avito_uploaded.name
        except Exception as exc:
            st.error(f"Ошибка файла Авито: {exc}")
    st.markdown(f'<div class="sidebar-status">Авито: {html.escape(st.session_state.get("avito_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Отчёт и цены", "📊", "Порог выгоды и минимальный остаток для пересчёта лучшей цены.")
    st.number_input("Порог отчёта, %", min_value=0.0, max_value=95.0, step=1.0, key="distributor_threshold")
    st.number_input("Мин. остаток у поставщика", min_value=1.0, max_value=999999.0, step=1.0, key="distributor_min_qty")
    st.markdown('<div class="sidebar-mini">Колонки Мин. у конкурентов / Разница из Excel не используются. Всё считаем заново прямо в приложении.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Быстрая правка цен", "✏️", "Меняет Наша цена в загруженном листе. Полезно для локальной проверки без правки исходного Excel.")
    st.text_area("Правка цен", key="price_patch_input", height=110, label_visibility="collapsed", placeholder="CE278A 8900\nCF364A - 29700")
    if st.button("Править цены в листе", use_container_width=True):
        current_df = st.session_state.get("current_df")
comparison_name = st.session_state.get("comparison_name", "ещё не загружен")
sheets = st.session_state.get("comparison_sheets", {})
loaded_sheet_count = len(sheets) if isinstance(sheets, dict) else 0
rows_count = sum(len(df) for df in sheets.values()) if isinstance(sheets, dict) and sheets else 0
price_mode = st.session_state.price_mode
round100 = st.session_state.round100
custom_discount = float(st.session_state.custom_discount)
search_mode = st.session_state.search_mode
price_label = current_price_label(price_mode, custom_discount)

st.markdown(f"""
<div class="topbar"><div class="topbar-grid">
<div class="brand-box"><div class="logo">📦</div><div><div class="brand-title">{APP_TITLE}</div><div class="brand-sub">Один comparison-файл • поиск • фото • пересчёт цен поставщиков</div></div></div>
<div class="stat-box"><div class="stat-cap">Файл</div><div class="stat-val">{html.escape(comparison_name)}</div></div>
<div class="stat-box"><div class="stat-cap">Вкладок</div><div class="stat-val">{loaded_sheet_count if loaded_sheet_count else '—'}</div></div>
<div class="stat-box"><div class="stat-cap">Всего строк</div><div class="stat-val">{rows_count}</div></div>
</div></div>
""", unsafe_allow_html=True)


def render_sheet_workspace(sheet_name: str, tab_label: str, tab_key: str) -> None:
    base_sheet_df = sheets.get(sheet_name) if isinstance(sheets, dict) else None
    sheet_df = apply_photo_map(base_sheet_df, st.session_state.get("photo_df")) if isinstance(base_sheet_df, pd.DataFrame) else None
    source_pairs = get_source_pairs(sheet_df) if isinstance(sheet_df, pd.DataFrame) else []
    search_key = f"search_input_{tab_key}"
    submitted_key = f"submitted_query_{tab_key}"
    result_key = f"last_result_{tab_key}"
    if search_key not in st.session_state:
        st.session_state[search_key] = ""
    if submitted_key not in st.session_state:
        st.session_state[submitted_key] = ""
    if result_key not in st.session_state:
        st.session_state[result_key] = None

    st.markdown('<div class="toolbar">', unsafe_allow_html=True)
    render_block_header(
        f"{tab_label} — поиск товара",
        f"Работа только по листу «{sheet_name}». Сначала артикул, потом название, если это разрешено режимом поиска.",
        icon="🔎",
        help_text="Приложение больше не читает сырые прайсы дистрибьюторов. Оно ищет внутри текущего листа comparison-файла и по найденным строкам пересчитывает лучшую цену поставщика по колонкам вида 'Источник цена' / 'Источник шт'.",
    )
    with st.form(f"search_form_{tab_key}", clear_on_submit=False):
        search_value = st.text_area(
            "Поисковый запрос",
            value=st.session_state[search_key],
            placeholder="Например:\nCE278A CE285A\nили\n001R00600 / 006R01464",
            height=90,
            label_visibility="collapsed",
        )
        c1, c2, c3 = st.columns([1, 1, 2.4])
        find_clicked = c1.form_submit_button("🔎 Найти", use_container_width=True, type="primary")
        clear_clicked = c2.form_submit_button("🧹 Очистить", use_container_width=True)
        c3.markdown(
            f"<div style='padding-top:9px;color:#64748b;font-size:12px;'>Тип поиска сейчас: <b>{html.escape(search_mode)}</b>. Короткие OEM-коды вроде TK-8600Y лучше искать режимом «Артикул + коды из названия».</div>",
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)

    if clear_clicked:
        st.session_state[search_key] = ""
        st.session_state[submitted_key] = ""
        st.session_state[result_key] = None
        st.rerun()

    if find_clicked:
        normalized_query = normalize_query_for_display(search_value)
        st.session_state[search_key] = normalized_query
        st.session_state[submitted_key] = normalized_query
        st.session_state[result_key] = search_in_df(sheet_df, normalized_query, search_mode) if isinstance(sheet_df, pd.DataFrame) else None
        st.rerun()

    submitted_query = st.session_state.get(submitted_key, "")
    result_df = st.session_state.get(result_key)
    min_dist_qty = float(st.session_state.get("distributor_min_qty", 1.0))

    if sheet_name == "Сравнение":
        render_info_banner(
            "Защита от совместимки и мусорных цен",
            "Во вкладке Оригинал сначала скрываются цены поставщиков, которые уже совпали с листом Совместимые по OEM-коду. Дополнительно отсекаются экстремально низкие выбросы, если они резко выпадают из нормального коридора цен.",
            icon="🛡️",
            chips=["сначала фильтр по совместимым", "потом отсев аномально низких цен", "оригинальная строка остаётся"],
            tone="green",
        )

    if not isinstance(sheet_df, pd.DataFrame):
        render_info_banner(
            f"Вкладка «{tab_label}» пока пуста",
            f"В comparison-файле не найден лист «{sheet_name}».",
            icon="📭",
            chips=["проверь названия листов", "ожидаются: Сравнение / Уценка / Совместимые"],
            tone="purple",
        )
        return

    if result_df is None:
        render_info_banner(
            f"{tab_label}: лист загружен",
            f"Теперь введите артикул или несколько артикулов для поиска по листу «{sheet_name}».",
            icon="✅",
            chips=[f"строк: {len(sheet_df)}", "фото в таблице", "цены поставщиков считаются заново"],
            tone="green",
        )
    else:
        st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
        render_block_header(
            f"{tab_label} — результаты поиска",
            "Главная таблица по найденным позициям. Справа теперь фото, а не кнопка копирования цены.",
            icon="📋",
            help_text="Если у товара есть фото по артикулу в отдельном файле, оно покажется прямо в строке результата. Если фото нет, будет заглушка.",
        )
        if result_df.empty:
            st.warning("Ничего не найдено. Попробуйте другой артикул или часть названия.")
        else:
            compare_map = build_distributor_compare(result_df, min_qty=min_dist_qty)
            render_results_insight_dashboard(result_df, compare_map, source_pairs)
            render_results_table(result_df.head(200), price_mode, round100, custom_discount, distributor_map=compare_map)
            st.download_button(
                "⬇️ Скачать результаты в Excel",
                to_excel_bytes(result_df, price_mode, round100, custom_discount, min_dist_qty),
                file_name=f"moy_tovar_search_results_{tab_key}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"download_results_{tab_key}",
            )
            with st.expander("Показать техническую таблицу"):
                tech = result_df.copy()
                tech["Наша цена"] = tech["sale_price"].map(fmt_price)
                tech["Наш склад"] = tech["free_qty"].map(fmt_qty)
                tech["Лучшая цена"] = tech.apply(lambda row: (get_best_offer(row, min_qty=min_dist_qty) or {}).get("price_fmt", ""), axis=1)
                tech["Лучший поставщик"] = tech.apply(lambda row: (get_best_offer(row, min_qty=min_dist_qty) or {}).get("source", ""), axis=1)
                tech["Фото"] = tech.get("photo_url", "")
                tech = tech[["article", "name", "Наша цена", "Наш склад", "Лучший поставщик", "Лучшая цена", "Фото"]].rename(columns={"article": "Артикул", "name": "Название"})
                st.dataframe(tech, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

        if not result_df.empty:
            st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
            render_block_header(
                f"{tab_label} — показать цены у всех",
                "Здесь для каждой найденной позиции показываются все доступные поставщики из колонок текущего comparison-листа.",
                icon="🏷️",
            )
            render_info_banner(
                "Что здесь важно",
                "Берём только пары колонок 'Источник цена' и 'Источник шт'. Готовые поля 'Мин. у конкурентов' и 'Разница' из Excel не используются вообще.",
                icon="🧠",
                chips=["свои расчёты", "динамические источники", "работает и для новых колонок"],
                tone="green",
            )
            render_all_prices_block(result_df, min_dist_qty, price_mode, round100, custom_discount)
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
            render_block_header(
                f"{tab_label} — шаблоны",
                "Два быстрых шаблона для ответа или публикации по найденным позициям.",
                icon="🧾",
            )
            t1, t2 = st.columns(2)
            with t1:
                template1 = build_offer_template(sheet_df, submitted_query, round100, st.session_state.template1_footer, search_mode)
                st.text_area("Шаблон 1", value=template1, height=300, key=f"template1_{tab_key}")
            with t2:
                template2 = build_selected_price_template(sheet_df, submitted_query, price_mode, round100, custom_discount, search_mode)
                st.text_area("Шаблон 2", value=template2, height=300, key=f"template2_{tab_key}")
            st.markdown('</div>', unsafe_allow_html=True)

            if isinstance(st.session_state.get("avito_df"), pd.DataFrame) and not st.session_state.avito_df.empty:
                st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
                render_block_header(
                    f"{tab_label} — Авито",
                    "Проверка, есть ли по найденным артикулам объявления в загруженном файле Авито.",
                    icon="🛒",
                )
                render_avito_block(st.session_state.avito_df, result_df)
                st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
    render_block_header(
        f"{tab_label} — отчёт по листу",
        "Полный отчёт по выбранному листу: где поставщик реально дешевле нас на заданный процент.",
        icon="📊",
        help_text="Отчёт строится по всему текущему листу, а не только по поисковой выдаче. Порог и минимальный остаток меняются в sidebar.",
    )
    report_df = build_report_df(sheet_df, st.session_state.distributor_threshold, st.session_state.distributor_min_qty)
    if report_df.empty:
        st.info("По текущему листу нет позиций, которые проходят ваш порог выгоды.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Строк в отчёте", len(report_df))
        c2.metric("Порог", f"{fmt_qty(st.session_state.distributor_threshold)}%")
        c3.metric("Источников", len(source_pairs))
        st.dataframe(report_df, use_container_width=True, hide_index=True, height=420)
        st.download_button(
            "⬇️ Скачать отчёт по листу",
            report_to_excel_bytes(report_df),
            file_name=f"moy_tovar_report_{tab_key}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"download_report_{tab_key}",
        )
    st.markdown('</div>', unsafe_allow_html=True)


if not isinstance(sheets, dict) or not sheets:
    render_info_banner(
        "С чего начать",
        "Загрузите comparison-файл. После этого сверху появятся 3 вкладки: Оригинал, Уценка и Совместимые. Затем можно подключить файл фото и искать позиции.",
        icon="🚀",
        chips=["один файл вместо многих", "3 отдельные вкладки", "фото по артикулу"],
        tone="purple",
    )
else:
    tab_specs = [
        ("Сравнение", "Оригинал", "original"),
        ("Уценка", "Уценка", "discount"),
        ("Совместимые", "Совместимые", "compatible"),
    ]
    tabs = st.tabs([label for _, label, _ in tab_specs])
    for tab, (sheet_name, tab_label, tab_key) in zip(tabs, tab_specs):
        with tab:
            render_sheet_workspace(sheet_name, tab_label, tab_key)
