
from __future__ import annotations

import html
import io
import json
import math
import re
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from collections import Counter, defaultdict
from urllib.parse import quote_plus, unquote, urljoin, urlparse

REQUESTS_IMPORT_ERROR: str | None = None
try:
    import requests
except ImportError as exc:
    requests = None
    REQUESTS_IMPORT_ERROR = str(exc)

import openpyxl
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Мой Товар", page_icon="📦", layout="wide")

APP_TITLE = "Мой Товар"
APP_VERSION = "v54.6.1"


SERVER_DATA_DIRNAME = "data"
PERSISTED_PHOTO_FILENAME = "photo_catalog_latest.xlsx"
PERSISTED_AVITO_FILENAME = "avito_latest.xlsx"
PERSISTED_COMPARISON_FILENAME = "comparison_latest.xlsx"
PERSISTED_WATCHLIST_FILENAME = "hot_items_watchlist_latest.dat"
PERSISTED_META_SUFFIX = ".meta.json"

FALLBACK_PHOTO_DOMAINS = ["rashodniki.ru", "t-toner.ru", "interlink.ru", "mrimage.ru"]
FALLBACK_SEARCH_LIMIT = 2


def get_server_data_dir() -> Path:
    try:
        base = Path(__file__).resolve().with_name(SERVER_DATA_DIRNAME)
    except Exception:
        base = Path.cwd() / SERVER_DATA_DIRNAME
    base.mkdir(parents=True, exist_ok=True)
    return base


def get_persisted_photo_file_path() -> Path:
    return get_server_data_dir() / PERSISTED_PHOTO_FILENAME


def get_persisted_avito_file_path() -> Path:
    return get_server_data_dir() / PERSISTED_AVITO_FILENAME


def get_persisted_comparison_file_path() -> Path:
    return get_server_data_dir() / PERSISTED_COMPARISON_FILENAME


def get_persisted_watchlist_file_path() -> Path:
    return get_server_data_dir() / PERSISTED_WATCHLIST_FILENAME


def get_persisted_meta_path(file_path: Path) -> Path:
    return file_path.with_suffix(file_path.suffix + PERSISTED_META_SUFFIX)


def read_persisted_original_name(file_path: Path, default_name: str) -> str:
    meta_path = get_persisted_meta_path(file_path)
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            name = normalize_text(meta.get("original_name", ""))
            if name:
                return name
        except Exception:
            pass
    return default_name


def save_uploaded_source_file(target_path: Path, file_bytes: bytes, original_name: str) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(file_bytes)
    meta_path = get_persisted_meta_path(target_path)
    meta_path.write_text(json.dumps({
        "original_name": normalize_text(original_name),
        "saved_at": datetime.utcnow().isoformat(timespec="seconds"),
        "size": len(file_bytes),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

def clear_loader_caches() -> None:
    """Сбрасываем только тяжёлые loader-кеши после обновления server-side файлов."""
    try:
        load_comparison_workbook.clear()
    except Exception:
        pass
    try:
        load_photo_map_file.clear()
    except Exception:
        pass
    try:
        load_avito_file.clear()
    except Exception:
        pass
    try:
        load_hot_watchlist_file.clear()
    except Exception:
        pass


def log_operation(message: str, level: str = "info") -> None:
    try:
        if "operation_log" not in st.session_state or not isinstance(st.session_state.get("operation_log"), list):
            st.session_state["operation_log"] = []
        stamp = datetime.utcnow().strftime("%H:%M:%S")
        st.session_state["operation_log"].append({"time": stamp, "level": level, "message": normalize_text(message)})
        st.session_state["operation_log"] = st.session_state["operation_log"][-25:]
    except Exception:
        pass


def render_operation_log_sidebar() -> None:
    log_items = st.session_state.get("operation_log", [])
    with st.expander("История действий", expanded=False):
        if not log_items:
            st.caption("Пока пусто")
            return
        for item in reversed(log_items[-12:]):
            icon = "✅" if item.get("level") == "success" else ("⚠️" if item.get("level") == "warning" else "•")
            st.markdown(f"{icon} **{html.escape(str(item.get('time', '')))}** — {html.escape(str(item.get('message', '')))}", unsafe_allow_html=True)



def load_persisted_photo_source_into_state() -> bool:
    target = get_persisted_photo_file_path()
    if not target.exists():
        return False
    try:
        raw = target.read_bytes()
        df = load_photo_map_file(read_persisted_original_name(target, target.name), raw)
        st.session_state.photo_df = df
        st.session_state.photo_name = read_persisted_original_name(target, target.name) + " • из /data"
        return True
    except Exception:
        return False


def load_persisted_avito_source_into_state() -> bool:
    target = get_persisted_avito_file_path()
    if not target.exists():
        return False
    try:
        raw = target.read_bytes()
        st.session_state.avito_df = load_avito_file(read_persisted_original_name(target, target.name), raw)
        st.session_state.avito_name = read_persisted_original_name(target, target.name) + " • из /data"
        return True
    except Exception:
        return False


def load_persisted_comparison_source_into_state() -> bool:
    target = get_persisted_comparison_file_path()
    if not target.exists():
        return False
    try:
        raw = target.read_bytes()
        wb = load_comparison_workbook(read_persisted_original_name(target, target.name), raw)
        st.session_state.comparison_sheets = wb
        st.session_state.comparison_name = read_persisted_original_name(target, target.name) + " • из /data"
        st.session_state.comparison_version = datetime.utcnow().isoformat()
        available = list(wb.keys())
        if available and st.session_state.get("selected_sheet", "Сравнение") not in available:
            st.session_state.selected_sheet = available[0]
        rebuild_current_df()
        refresh_all_search_results()
        return True
    except Exception:
        return False


def normalize_watchlist_sheet_name(value: Any) -> str:
    txt = contains_text(value)
    if "ОРИГИН" in txt or "СРАВН" in txt:
        return "Оригинал"
    if "УЦЕН" in txt:
        return "Уценка"
    if "СОВМЕСТ" in txt:
        return "Совместимые"
    return normalize_text(value)


@st.cache_data(show_spinner=False, ttl=3600, max_entries=4)
def load_hot_watchlist_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix == ".csv":
        bio = io.BytesIO(file_bytes)
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
    else:
        raw = pd.read_excel(io.BytesIO(file_bytes))
    raw = raw.dropna(how="all").copy()
    if raw.empty:
        return pd.DataFrame(columns=[
            "watch_article", "watch_key", "watch_name", "current_sheet", "comparison_article",
            "sales_qty_15m", "sales_per_month", "abc_class", "velocity_band",
            "best_supplier", "best_supplier_gap_pct", "buy_signal_30pct", "days_of_cover",
            "priority_score", "action_today", "watch_article_norm", "watch_key_norm",
            "comparison_article_norm", "match_keys_text",
        ])
    raw.columns = [normalize_text(c) for c in raw.columns]
    rows = []
    for _, r in raw.iterrows():
        watch_article = normalize_text(r.get("watch_article", ""))
        watch_key = normalize_text(r.get("watch_key", ""))
        watch_name = normalize_text(r.get("watch_name", ""))
        comparison_article = normalize_text(r.get("comparison_article", ""))
        keys = unique_preserve_order([
            normalize_article(watch_article),
            normalize_article(watch_key),
            normalize_article(comparison_article),
        ])
        if not any(keys):
            continue
        rows.append({
            "watch_article": watch_article,
            "watch_key": watch_key,
            "watch_name": watch_name,
            "current_sheet": normalize_watchlist_sheet_name(r.get("current_sheet", "")),
            "comparison_article": comparison_article,
            "sales_qty_15m": safe_float(r.get("sales_qty_15m"), 0.0),
            "sales_per_month": safe_float(r.get("sales_per_month"), 0.0),
            "abc_class": normalize_text(r.get("abc_class", "")),
            "velocity_band": normalize_text(r.get("velocity_band", "")),
            "ledger_end_qty": safe_float(r.get("ledger_end_qty"), 0.0),
            "our_price_now": safe_float(r.get("our_price_now"), 0.0),
            "our_stock_now": safe_float(r.get("our_stock_now"), 0.0),
            "best_supplier": normalize_text(r.get("best_supplier", "")),
            "best_supplier_price_now": safe_float(r.get("best_supplier_price_now"), 0.0),
            "best_supplier_stock_now": safe_float(r.get("best_supplier_stock_now"), 0.0),
            "best_supplier_gap_pct": safe_float(r.get("best_supplier_gap_pct"), 0.0),
            "buy_signal_30pct": normalize_text(r.get("buy_signal_30pct", "")),
            "days_of_cover": safe_float(r.get("days_of_cover"), 0.0),
            "priority_score": safe_float(r.get("priority_score"), 0.0),
            "action_today": normalize_text(r.get("action_today", "")),
            "watch_article_norm": normalize_article(watch_article),
            "watch_key_norm": normalize_article(watch_key),
            "comparison_article_norm": normalize_article(comparison_article),
            "match_keys_text": "|".join([k for k in keys if k]),
        })
    out = pd.DataFrame(rows)
    return out.reset_index(drop=True)


def build_hot_watchlist_lookup(hot_df: pd.DataFrame | None, tab_label: str = "") -> dict[str, list[dict[str, Any]]]:
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return {}
    work = hot_df.copy()
    tab_label = normalize_text(tab_label)
    if tab_label:
        filtered = work[(work["current_sheet"] == "") | (work["current_sheet"] == tab_label)].copy()
        if not filtered.empty:
            work = filtered
    lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for _, row in work.iterrows():
        rec = row.to_dict()
        keys = [normalize_article(x) for x in normalize_text(rec.get("match_keys_text", "")).split("|") if normalize_article(x)]
        if not keys:
            continue
        for key in keys:
            lookup[key].append(rec)
    return lookup


def pick_hot_watch_rec(row: pd.Series, lookup: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    if not lookup:
        return None
    candidate_keys = []
    article_norm = normalize_article(row.get("article_norm", row.get("article", "")))
    if article_norm:
        candidate_keys.append(article_norm)
    row_codes = row.get("row_codes")
    if isinstance(row_codes, list):
        candidate_keys.extend([normalize_article(x) for x in row_codes if normalize_article(x)])
    best = None
    best_score = -10**18
    seen = set()
    for key in candidate_keys:
        if not key or key in seen:
            continue
        seen.add(key)
        for rec in lookup.get(key, []):
            score = safe_float(rec.get("priority_score"), 0.0)
            if normalize_text(rec.get("buy_signal_30pct", "")).upper() == "BUY":
                score += 100000.0
            if key == normalize_article(rec.get("comparison_article_norm", "")):
                score += 1000.0
            elif key == normalize_article(rec.get("watch_article_norm", "")):
                score += 500.0
            if score > best_score:
                best = rec
                best_score = score
    return best


def apply_hot_watchlist(df: pd.DataFrame | None, hot_df: pd.DataFrame | None, tab_label: str = "") -> pd.DataFrame | None:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return df
    lookup = build_hot_watchlist_lookup(hot_df, tab_label=tab_label)
    if not lookup:
        return df
    work = df.copy()
    matches = [pick_hot_watch_rec(row, lookup) for _, row in work.iterrows()]
    work["hot_flag"] = [bool(m) for m in matches]
    work["hot_sales_per_month"] = [safe_float((m or {}).get("sales_per_month"), 0.0) for m in matches]
    work["hot_priority_score"] = [safe_float((m or {}).get("priority_score"), 0.0) for m in matches]
    work["hot_abc_class"] = [normalize_text((m or {}).get("abc_class", "")) for m in matches]
    work["hot_velocity_band"] = [normalize_text((m or {}).get("velocity_band", "")) for m in matches]
    work["hot_action_today"] = [normalize_text((m or {}).get("action_today", "")) for m in matches]
    work["hot_buy_signal"] = [normalize_text((m or {}).get("buy_signal_30pct", "")) for m in matches]
    work["hot_best_supplier"] = [normalize_text((m or {}).get("best_supplier", "")) for m in matches]
    work["hot_best_supplier_gap_pct"] = [safe_float((m or {}).get("best_supplier_gap_pct"), 0.0) for m in matches]
    work["hot_watch_article"] = [normalize_text((m or {}).get("watch_article", "")) for m in matches]
    return work




def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Watchlist")
    return out.getvalue()

def hot_watchlist_summary_text() -> str:
    hot_df = st.session_state.get("hot_items_df")
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return "watchlist не загружен"
    buy_count = int((hot_df.get("buy_signal_30pct", pd.Series(dtype=object)).fillna("").map(normalize_text).str.upper() == "BUY").sum())
    ab_count = int(hot_df.get("abc_class", pd.Series(dtype=object)).fillna("").map(normalize_text).isin(["A", "B"]).sum())
    return f"Ходовых: {len(hot_df)} • сильный спрос: {ab_count} • можно брать: {buy_count}"

def build_hot_buy_watchlist_table() -> pd.DataFrame:
    hot_df = st.session_state.get("hot_items_df")
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return pd.DataFrame()
    work = hot_df.copy()
    buy_mask = work.get("buy_signal_30pct", pd.Series(dtype=object)).fillna("").map(normalize_text).str.upper().eq("BUY")
    work = work.loc[buy_mask].copy()
    if work.empty:
        return pd.DataFrame()
    work = work[
        pd.to_numeric(work.get("our_price_now", 0.0), errors="coerce").fillna(0.0).gt(0)
        & pd.to_numeric(work.get("best_supplier_price_now", 0.0), errors="coerce").fillna(0.0).gt(0)
    ].copy()
    if work.empty:
        return pd.DataFrame()
    work["gap_pct_display"] = work.get("best_supplier_gap_pct", pd.Series(dtype=float)).fillna(0.0).map(lambda x: round(float(x) * 100.0, 1))
    out = pd.DataFrame({
        "Лист": work.get("current_sheet", ""),
        "Артикул": work.get("comparison_article", work.get("watch_article", "")),
        "Товар": work.get("watch_name", ""),
        "Ходовая": "Да",
        "Спрос, шт/мес": work.get("sales_per_month", 0.0),
        "Наша цена": work.get("our_price_now", 0.0),
        "Наш остаток": work.get("our_stock_now", 0.0),
        "Лучший поставщик": work.get("best_supplier", ""),
        "Цена поставщика": work.get("best_supplier_price_now", 0.0),
        "Остаток поставщика": work.get("best_supplier_stock_now", 0.0),
        "Ниже нашей цены, %": work.get("gap_pct_display", 0.0),
        "Дней запаса": work.get("days_of_cover", 0.0),
        "Приоритет": work.get("priority_score", 0.0),
        "Действие": [translate_watch_action(x, threshold_pct=35.0) for x in work.get("action_today", "")],
    })
    for col in ["Спрос, шт/мес", "Наша цена", "Наш остаток", "Цена поставщика", "Остаток поставщика", "Ниже нашей цены, %", "Дней запаса", "Приоритет"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values(["Приоритет", "Спрос, шт/мес"], ascending=[False, False], kind="stable").reset_index(drop=True)
    return out


def render_hot_buy_watchlist_lazy_panel() -> None:
    active_sheet_name = normalize_text(st.session_state.get("active_workspace_sheet_name", ""))
    header_key = f"crm_show_buy_{active_sheet_name}" if active_sheet_name else ""
    is_open = bool(st.session_state.get("show_hot_buy_watchlist_table", False)) or bool(st.session_state.get(header_key, False))
    if not is_open:
        return
    buy_df = build_hot_buy_watchlist_table()
    st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
    render_block_header(
        "Ходовые позиции — сейчас можно брать",
        "Ленивая таблица только по ходовым позициям, где лучший поставщик минимум на 35% дешевле нашей цены.",
        icon="🔥",
        help_text="Показывает только ходовые позиции, где лучший поставщик сейчас минимум на 35% дешевле нашей цены. Таблица не грузится, пока чекбокс в блоке Watchlist выключен.",
    )
    if buy_df.empty:
        st.info("Сейчас в watchlist нет позиций, где лучший поставщик минимум на 35% дешевле нашей цены.")
    else:
        c1, c2 = st.columns([1.2, 1])
        c1.metric("Позиций можно брать", len(buy_df))
        c2.download_button(
            "⬇️ Скачать таблицу «можно брать» в Excel",
            dataframe_to_excel_bytes(buy_df),
            file_name="hot_buy_watchlist.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_hot_buy_watchlist",
            use_container_width=True,
        )

        f1, f2, f3, f4 = st.columns([1.15, 1.1, 1.2, 1.35])
        sheet_options = ["Все листы"] + sorted(
            [x for x in buy_df["Лист"].fillna("").astype(str).unique().tolist() if x.strip()]
        )
        selected_sheet = f1.selectbox(
            "Лист",
            sheet_options,
            key="hot_buy_sheet_filter",
            help="Ограничивает таблицу выбранным листом: Оригинал, Уценка или Совместимые.",
        )
        only_hot = f2.checkbox(
            "Только ходовые",
            value=True,
            key="hot_buy_only_hot",
            help="Показывать только товары с хорошим спросом за выбранный период.",
        )
        only_buy = f3.checkbox(
            "Только можно брать",
            value=True,
            key="hot_buy_only_buy",
            help="Показывать только позиции, где лучший поставщик сейчас минимум на 35% дешевле нашей цены.",
        )
        only_attention = f4.checkbox(
            "Только требует внимания",
            value=False,
            key="hot_buy_only_attention",
            help="Показывать позиции, где кроме выгодной закупки есть ещё действие: пополнить запас, наблюдать или проверить сравнение.",
        )

        st.caption(
            "Лист — ограничивает таблицу выбранным разделом. "
            "Только ходовые — товары с хорошим спросом. "
            "Только можно брать — позиции, где поставщик сейчас минимум на 35% дешевле нашей цены. "
            "Только требует внимания — позиции, где кроме выгодной закупки есть ещё действие: пополнить запас, наблюдать или проверить сравнение."
        )

        filtered_buy_df = buy_df.copy()

        if selected_sheet != "Все листы":
            filtered_buy_df = filtered_buy_df[filtered_buy_df["Лист"].astype(str) == selected_sheet]

        if only_hot:
            filtered_buy_df = filtered_buy_df[filtered_buy_df["Ходовая"].astype(str).str.strip().eq("Да")]

        if only_buy:
            filtered_buy_df = filtered_buy_df[
                filtered_buy_df["Действие"].fillna("").astype(str).str.contains("Можно брать", regex=False)
            ]

        if only_attention:
            filtered_buy_df = filtered_buy_df[
                filtered_buy_df["Действие"].fillna("").astype(str).str.contains(
                    "Пополнить запас|Наблюдать|Нет в сравнении", regex=True
                )
            ]

        if filtered_buy_df.empty:
            st.info("По выбранным фильтрам строк не найдено.")
        else:
            st.dataframe(filtered_buy_df, use_container_width=True, height=min(640, 80 + len(filtered_buy_df) * 35))
    st.markdown('</div>', unsafe_allow_html=True)


def hot_supplier_note(row: pd.Series | dict | None, best: dict | None, threshold_pct: float = 35.0) -> tuple[str, str]:
    help_text = "Товар ходовой → товар хорошо продавался за выбранный период"
    if not best:
        help_text += f"\nСейчас брать невыгодно → нет поставщика с ценой минимум на {threshold_pct:.0f}% ниже нашей цены"
        return "Сейчас брать невыгодно", help_text

    source = normalize_text((best or {}).get("source", ""))
    delta_pct = safe_float((best or {}).get("delta_percent"), 0.0)
    if delta_pct >= float(threshold_pct):
        action_text = f"Сейчас можно брать у {source}" if source else "Сейчас можно брать"
        help_text += f"\nСейчас можно брать → лучший поставщик сейчас минимум на {threshold_pct:.0f}% дешевле нашей цены"
        return action_text, help_text

    help_text += f"\nСейчас брать невыгодно → нет поставщика с ценой минимум на {threshold_pct:.0f}% ниже нашей цены"
    return "Сейчас брать невыгодно", help_text

def load_persisted_watchlist_source_into_state() -> bool:
    target = get_persisted_watchlist_file_path()
    if not target.exists():
        return False
    try:
        raw = target.read_bytes()
        st.session_state.hot_items_df = load_hot_watchlist_file(read_persisted_original_name(target, target.name), raw)
        st.session_state.hot_items_name = read_persisted_original_name(target, target.name) + " • из /data"
        return True
    except Exception:
        return False

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
    "total_qty": ["Всего", "Итого", "Общий остаток", "Всего шт", "Итого шт"],
    "transit_qty": ["Транзит", "В транзите", "В пути", "Поступает", "Транзит шт"],
}

PHOTO_COLUMN_ALIASES = {
    "article": ["Артикул", "артикул", "Код", "код", "sku", "article"],
    "photo_url": [
        "Фото", "Ссылка на фото", "URL фото", "photo", "image", "image_url",
        "photo_url", "url", "link", "picture", "картинка", "ссылка",
        "imag", "images"
    ],
    "color": ["czvet", "цвет", "color"],
    "iso_pages": ["resurs-po-iso-str", "ресурс-по-iso-стр", "ресурс", "iso", "pages"],
    "manufacturer_code": ["kod-proizvoditelya", "код-производителя", "код производителя"],
    "model": ["model", "модель"],
    "fits_models": ["podhodit-k-modelyam", "подходит-к-моделям", "подходит к моделям"],
}

AVITO_COLUMN_ALIASES = {
    "ad_id": ["Номер объявления", "ID объявления", "Номер"],
    "title": ["Название объявления", "Заголовок", "Название"],
    "price": ["Цена"],
    "url": ["Ссылка", "URL", "Ссылка на объявление", "Link"],
    "account": ["Аккаунт", "account", "Кабинет", "Профиль"],
}

CYRILLIC_ARTICLE_TRANSLATION = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "а": "A", "в": "B", "е": "E", "к": "K", "м": "M", "н": "H", "о": "O", "р": "P", "с": "C", "т": "T", "у": "Y", "х": "X",
    "Ё": "E", "ё": "E",
})

MERLION_PANTUM_EXTRA_ZERO_CODES = {
    "DL420P", "DL5120P", "DLR5220",
    "TL420HP", "TL420XP", "TL5120HP", "TL5120P", "TL5120XP",
}


def normalize_merlion_source_price(row: pd.Series, source: str, price: float) -> float:
    """
    Безопасная правка только для проблемной группы Pantum у Мерлиона.
    Делим на 10 только когда:
    - источник именно Мерлион
    - в строке есть Pantum
    - найден один из известных OEM-кодов
    - цена выглядит как явно раздутая: 51000 / 73500 / 122000 и т.п.
    """
    try:
        price_val = float(price)
    except Exception:
        return float(price)

    if compact_text(source) != "МЕРЛИОН":
        return price_val
    if price_val < 10000:
        return price_val

    row_text = contains_text(f"{row.get('article', '')} {row.get('name', '')}")
    if "PANTUM" not in row_text:
        return price_val

    row_codes = row.get("row_codes")
    if not isinstance(row_codes, list) or not row_codes:
        row_codes = build_row_compare_codes(row.get("article", ""), row.get("name", ""))
    if not any(normalize_article(code) in MERLION_PANTUM_EXTRA_ZERO_CODES for code in (row_codes or [])):
        return price_val

    rounded = int(round(price_val))
    if abs(price_val - rounded) > 1e-9:
        return price_val
    if rounded % 100 != 0:
        return price_val

    return price_val / 10.0


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


COLOR_TEMPLATE_KEYWORDS = [
    ("пурп", "пурпурный"),
    ("magenta", "пурпурный (magenta)"),
    ("голуб", "голубой"),
    ("cyan", "голубой (cyan)"),
    ("желт", "желтый"),
    ("yellow", "желтый (yellow)"),
    ("черн", "черный"),
    ("чёрн", "чёрный"),
    ("black", "чёрный (black)"),
    ("син", "синий"),
    ("blue", "синий (blue)"),
    ("красн", "красный"),
    ("red", "красный (red)"),
    ("зел", "зеленый"),
    ("green", "зеленый (green)"),
    ("сер", "серый"),
    ("grey", "серый"),
    ("gray", "серый"),
]


def extract_color_from_text(value: object) -> str:
    text = contains_text(value)
    if not text:
        return ""
    for needle, label in COLOR_TEMPLATE_KEYWORDS:
        if contains_text(needle) in text:
            return label
    return ""


def extract_iso_pages_from_text(value: object) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    m = re.search(r"(\d[\d\s]{1,})\s*(?:стр|страниц)", raw, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"(\d[\d\s]{1,})\s*стр", raw, flags=re.IGNORECASE)
    if not m:
        return ""
    digits = re.sub(r"\s+", "", m.group(1))
    if not digits:
        return ""
    try:
        return f"{int(digits):,}".replace(",", " ")
    except Exception:
        return digits


def normalize_pages_value(value: object) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    # Берём только первое числовое значение и игнорируем хвосты вроде "(A4)" / "(А4)".
    # Иначе строка "2500 (A4)" превращается в ошибочные "25004".
    m = re.search(r"(\d[\d\s]{1,})", raw)
    if not m:
        m = re.search(r"(\d+)", raw)
    if m:
        digits = re.sub(r"\s+", "", m.group(1))
        if digits:
            try:
                return f"{int(digits):,}".replace(",", " ")
            except Exception:
                return digits
    return raw


def simplify_template_color(value: object) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    low = raw.lower().strip()

    english_only_map = {
        "black": "чёрный",
        "cyan": "голубой",
        "yellow": "жёлтый",
        "magenta": "пурпурный",
        "blue": "синий",
        "red": "красный",
        "green": "зелёный",
        "grey": "серый",
        "gray": "серый",
    }

    # Сначала нормализуем случаи, когда цвет пришёл только как английское слово
    # или только в скобках: magenta / (magenta) -> пурпурный
    raw_no_parens = re.sub(r"^[\s(]+|[\s)]+$", "", low)
    if raw_no_parens in english_only_map:
        return english_only_map[raw_no_parens]

    # Если цвет уже содержит русское название и английский перевод в скобках,
    # оставляем только один, более короткий и привычный вариант.
    replacements = [
        (r"^(ч[её]рн(?:ый|ая|ое)?)\s*\((?:black)\)$", "чёрный"),
        (r"^(черный)\s*\((?:black)\)$", "чёрный"),
        (r"^(голуб(?:ой|ая|ое)?)\s*\((?:cyan)\)$", "голубой"),
        (r"^(ж[её]лт(?:ый|ая|ое)?)\s*\((?:yellow)\)$", "жёлтый"),
        (r"^(пурпурн(?:ый|ая|ое)?)\s*\((?:magenta)\)$", "пурпурный"),
        (r"^(син(?:ий|яя|ее)?)\s*\((?:blue)\)$", "синий"),
        (r"^(красн(?:ый|ая|ое)?)\s*\((?:red)\)$", "красный"),
        (r"^(зел[её]н(?:ый|ая|ое)?)\s*\((?:green)\)$", "зелёный"),
        (r"^(сер(?:ый|ая|ое)?)\s*\((?:grey|gray)\)$", "серый"),
    ]
    for pattern, repl in replacements:
        if re.match(pattern, low, flags=re.IGNORECASE):
            return repl

    # Если есть русский цвет + английский перевод в конце, убираем перевод.
    if re.search(r"\(([A-Za-z]+)\)$", raw) and re.search(r"[А-Яа-яЁё]", raw):
        raw = re.sub(r"\s*\([A-Za-z]+\)$", "", raw).strip()
        return raw

    return raw


def compose_article_template_label(row: pd.Series) -> str:
    article = normalize_text(row.get("article", ""))
    color = simplify_template_color(normalize_text(row.get("meta_color", "")) or extract_color_from_text(row.get("name", "")))
    pages = normalize_pages_value(row.get("meta_iso_pages", "")) or extract_iso_pages_from_text(row.get("name", ""))
    details = []
    if color:
        details.append(color)
    if pages:
        details.append(f"{pages} стр")
    return f"{article} ({', '.join(details)})" if details else article


def unique_text_values(values: list[object]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        txt = normalize_text(value)
        if not txt:
            continue
        key = contains_text(txt)
        if key in seen:
            continue
        seen.add(key)
        out.append(txt)
    return out


def build_template_shared_lines(result_df: pd.DataFrame) -> list[str]:
    if result_df is None or result_df.empty:
        return []
    article_norms = {normalize_article(x) for x in result_df.get("article", []).tolist() if normalize_article(x)}
    model_values = unique_text_values(result_df.get("meta_model", pd.Series(dtype=object)).tolist())
    manufacturer_values = unique_text_values(result_df.get("meta_manufacturer_code", pd.Series(dtype=object)).tolist())
    fits_values = unique_text_values(result_df.get("meta_fits_models", pd.Series(dtype=object)).tolist())

    filtered_manufacturer = [v for v in manufacturer_values if normalize_article(v) not in article_norms]

    lines: list[str] = []
    if model_values:
        lines.append(f"Модель - {' / '.join(model_values)}")
    if filtered_manufacturer:
        lines.append(f"Код производителя - {' / '.join(filtered_manufacturer)}")
    if fits_values:
        lines.append(f"Подходит к моделям - {' / '.join(fits_values)}")
    return lines


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




def fmt_price_with_rub(value: Any) -> str:
    txt = fmt_price(value)
    return f"{txt} руб." if txt else ""

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


COMPATIBLE_BRAND_MARKERS: list[tuple[str, str]] = [
    ("NV PRINT", "NV Print"),
    ("STATIC CONTROL", "Static Control"),
    ("HI-BLACK", "Hi-Black"),
    ("NETPRODUCT", "NetProduct"),
    ("PROFILINE", "ProfiLine"),
    ("G&G", "G&G"),
    ("MYTONER", "MyToner"),
    ("MYTONE", "MyTone"),
    ("BLOSSOM", "Blossom"),
    ("CACTUS", "Cactus"),
    ("SAKURA", "Sakura"),
    ("KATUN", "Katun"),
    ("UNITON", "Uniton"),
    ("COPYRITE", "Copyrite"),
    ("COLORING", "Coloring"),
    ("BULAT", "Bulat"),
    ("CET", "CET"),
]

COMPATIBLE_ARTICLE_PREFIX_BRANDS = {
    "BS": "Blossom",
    "CS": "Cactus",
    "BSL": "Bulat",
    "CET": "CET",
    "NV": "NV Print",
    "SC": "Static Control",
    "STA": "Static Control",
    "KAT": "Katun",
    "KTN": "Katun",
    "SKR": "Sakura",
}


def extract_compatible_brand(name: object, article: object = "") -> str:
    text = contains_text(name)
    for needle, label in COMPATIBLE_BRAND_MARKERS:
        if needle in text:
            return label
    article_text = normalize_text(article)
    if article_text:
        prefix = article_text.split('-', 1)[0].strip().upper()
        if prefix in COMPATIBLE_ARTICLE_PREFIX_BRANDS:
            return COMPATIBLE_ARTICLE_PREFIX_BRANDS[prefix]
    return ""


SERIES_SUFFIX_ORDER = {"A": 0, "AC": 1, "X": 2, "XC": 3, "Y": 4, "YC": 5, "M": 6, "MC": 7, "C": 8, "K": 9}


def split_article_family_suffix(article_norm: str) -> tuple[str, str]:
    m = re.match(r"^(.*?\d)([A-ZА-Я]{1,3})$", article_norm)
    if m:
        return m.group(1), m.group(2)
    return article_norm, ""


def natural_chunks(value: str) -> list[object]:
    parts = re.split(r"(\d+)", value)
    result: list[object] = []
    for part in parts:
        if not part:
            continue
        result.append(int(part) if part.isdigit() else part)
    return result


def series_sort_key(candidate: dict[str, object]) -> tuple[object, ...]:
    article_norm = str(candidate.get("article_norm", ""))
    family, suffix = split_article_family_suffix(article_norm)
    rank = SERIES_SUFFIX_ORDER.get(suffix, 50)
    return (*natural_chunks(family), rank, suffix, article_norm)


def get_series_candidates(df: pd.DataFrame, raw_query: str) -> dict[str, object]:
    tokens = split_query_parts(raw_query)
    if len(tokens) != 1:
        return {"prefix": "", "candidates": []}
    token = tokens[0]
    token_norm = normalize_article(token)
    if len(token_norm) < 4:
        return {"prefix": token, "candidates": []}

    candidates_by_key: dict[str, dict[str, object]] = {}

    direct_df = df[df["article_norm"].str.startswith(token_norm, na=False)].copy()
    linked_mask = df["row_codes"].apply(lambda codes: any(str(code).startswith(token_norm) for code in (codes or [])) if isinstance(codes, list) else False)
    linked_df = df[linked_mask].copy()

    for source_df in [direct_df, linked_df]:
        for _, row in source_df.iterrows():
            candidate = {
                "article": str(row.get("article", "")),
                "article_norm": str(row.get("article_norm", "")),
                "name": str(row.get("name", "")),
                "free_qty": safe_float(row.get("free_qty", 0), 0.0),
                "sale_price": safe_float(row.get("sale_price", 0), 0.0),
                "original_block_reasons": list(row.get("original_block_reasons", []) or []),
            }
            if candidate["article_norm"] and candidate["article_norm"] not in candidates_by_key:
                candidates_by_key[candidate["article_norm"]] = candidate

    candidates = list(candidates_by_key.values())
    candidates.sort(key=series_sort_key)
    if len(candidates) < 2:
        return {"prefix": token, "candidates": []}
    return {"prefix": token, "candidates": candidates}


def build_sheet_code_reason_lookup(df: pd.DataFrame | None, reason: str) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = {}
    if df is None or df.empty:
        return lookup
    for _, row in df.iterrows():
        codes = row.get("row_codes")
        if not isinstance(codes, list) or not codes:
            codes = build_row_compare_codes(row.get("article", ""), row.get("name", ""))
        for code in codes:
            norm = normalize_article(code)
            if not norm:
                continue
            lookup.setdefault(norm, set()).add(reason)
    return lookup


def merge_code_reason_lookups(*lookups: dict[str, set[str]]) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = {}
    for lookup in lookups:
        for code, reasons in (lookup or {}).items():
            if not code or not reasons:
                continue
            merged.setdefault(code, set()).update(set(reasons))
    return {code: sorted(reasons) for code, reasons in merged.items() if reasons}


def get_original_block_reasons(codes: list[str], block_lookup: dict[str, list[str]]) -> list[str]:
    # Ограничения по Уценке/Совместимым отключены: comparison-файл считается уже поправленным.
    return []

def original_reason_badge_text(reasons: list[str]) -> str:
    if not isinstance(reasons, list) or not reasons:
        return ""
    order = []
    if "Уценка" in reasons:
        order.append("🟠 Уценка")
    if "Совместимые" in reasons:
        order.append("🟣 Совместимые")
    other = [r for r in reasons if r not in {"Уценка", "Совместимые"}]
    order.extend([f"⚪ {r}" for r in other])
    return " · ".join(order)


def original_reason_short_tag(reasons: list[str]) -> str:
    if not isinstance(reasons, list) or not reasons:
        return ""
    has_discount = "Уценка" in reasons
    has_compatible = "Совместимые" in reasons
    if has_discount and has_compatible:
        return "[скрыт: Уценка + Совместимые]"
    if has_discount:
        return "[скрыт: Уценка]"
    if has_compatible:
        return "[скрыт: Совместимые]"
    return "[скрыт]"


def original_reason_summary_html(hidden_reasons: dict[str, list[str]]) -> str:
    if not hidden_reasons:
        return ""
    only_discount = 0
    only_compatible = 0
    both = 0
    for reasons in hidden_reasons.values():
        has_discount = "Уценка" in reasons
        has_compatible = "Совместимые" in reasons
        if has_discount and has_compatible:
            both += 1
        elif has_discount:
            only_discount += 1
        elif has_compatible:
            only_compatible += 1
    chips = []
    if only_discount:
        chips.append(f"<span class='series-reason-chip chip-discount'>🟠 только Уценка: {only_discount}</span>")
    if only_compatible:
        chips.append(f"<span class='series-reason-chip chip-compatible'>🟣 только Совместимые: {only_compatible}</span>")
    if both:
        chips.append(f"<span class='series-reason-chip chip-both'>🔒 обе причины: {both}</span>")
    if not chips:
        return ""
    return "<div class='series-reason-row'>" + "".join(chips) + "</div>"


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
            price = normalize_merlion_source_price(row, source, price)
            qty = parse_qty_generic(row.get(pair.get("qty_col", "")))
            if not source or price <= 0 or qty <= 0:
                continue
            price_key = round(float(price), 2)
            for code in codes:
                lookup.setdefault(code, {}).setdefault(source, set()).add(price_key)
    return lookup


def merge_source_price_lookups(*lookups: dict[str, dict[str, set[float]]]) -> dict[str, dict[str, set[float]]]:
    merged: dict[str, dict[str, set[float]]] = {}
    for lookup in lookups:
        for code, source_map in (lookup or {}).items():
            code_norm = normalize_article(code)
            if not code_norm or not isinstance(source_map, dict):
                continue
            bucket = merged.setdefault(code_norm, {})
            for source, prices in source_map.items():
                source_name = str(source or "")
                if not source_name:
                    continue
                for price in prices or []:
                    price_val = safe_float(price, 0.0)
                    if price_val > 0:
                        bucket.setdefault(source_name, set()).add(round(float(price_val), 2))
    return merged


def merge_blocked_source_prices(codes: list[str], compatible_lookup: dict[str, dict[str, set[float]]]) -> dict[str, list[float]]:
    out: dict[str, set[float]] = {}
    for code in codes or []:
        for source, prices in compatible_lookup.get(code, {}).items():
            out.setdefault(source, set()).update(prices)
    return {source: sorted(values) for source, values in out.items() if values}


def is_blocked_by_compatible_price(row: pd.Series, source: str, price: float) -> bool:
    # Ограничения по совпадениям с Уценкой/Совместимыми сняты.
    return False


def filter_suspicious_low_offers(row: pd.Series, offers: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    # Дополнительный outlier-фильтр отключён: после правки comparison-файла показываем все цены как есть.
    return offers, []


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


@st.cache_data(show_spinner=False, ttl=3600, max_entries=6)
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
        total_col = mapping.get("total_qty")
        transit_col = mapping.get("transit_qty")
        df["total_qty"] = df[total_col].apply(parse_qty_generic) if total_col else df["free_qty"]
        df["transit_qty"] = df[transit_col].apply(parse_qty_generic) if transit_col else 0.0
        df["has_extended_stock"] = bool(total_col or transit_col)
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
    discount_df = sheets.get("Уценка")
    compatible_lookup = build_compatible_price_lookup(compatible_df)
    discount_lookup = build_compatible_price_lookup(discount_df)
    blocked_price_lookup = merge_source_price_lookups(compatible_lookup, discount_lookup)
    original_df = sheets.get("Сравнение")
    original_block_lookup = merge_code_reason_lookups(
        build_sheet_code_reason_lookup(discount_df, "Уценка"),
        build_sheet_code_reason_lookup(compatible_df, "Совместимые"),
    )
    if isinstance(original_df, pd.DataFrame) and not original_df.empty:
        original_df = original_df.copy()
        if blocked_price_lookup:
            original_df["blocked_source_prices"] = original_df["row_codes"].apply(lambda codes: merge_blocked_source_prices(codes, blocked_price_lookup))
        original_df["original_block_reasons"] = original_df["row_codes"].apply(lambda codes: get_original_block_reasons(codes, original_block_lookup))
        original_df["blocked_in_original"] = original_df["original_block_reasons"].map(lambda x: isinstance(x, list) and len(x) > 0)
        sheets["Сравнение"] = original_df
    return sheets


@st.cache_data(show_spinner=False, ttl=3600, max_entries=6)
def load_photo_map_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()

    def _sheet_priority(sheet_name: str) -> int:
        name = contains_text(sheet_name)
        if "ФОТО" in name or "СЫЛ" in name:
            return 0
        if "WORKSHEET" in name:
            return 20
        return 10

    def _empty_df() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            "article", "article_norm", "photo_url", "source_sheet", "sheet_priority",
            "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models",
        ])

    def _from_raw(raw: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
        raw = raw.dropna(how="all")
        if raw.empty:
            return _empty_df()
        raw = raw.copy()
        raw.columns = [normalize_text(c) for c in raw.columns]
        mapping = detect_mapping(raw, PHOTO_COLUMN_ALIASES)

        if not mapping.get("article"):
            for col in raw.columns:
                if compact_text(col) == "АРТИКУЛ":
                    mapping["article"] = col
                    break

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

        if not mapping.get("article"):
            return _empty_df()

        out = pd.DataFrame()
        out["article"] = raw[mapping["article"]].map(normalize_text)
        out["article_norm"] = raw[mapping["article"]].map(normalize_article)
        out["photo_url"] = raw[mapping["photo_url"]].map(extract_first_url) if mapping.get("photo_url") else ""
        out["source_sheet"] = sheet_name
        out["sheet_priority"] = _sheet_priority(sheet_name)
        out["meta_color"] = raw[mapping["color"]].map(normalize_text) if mapping.get("color") else ""
        out["meta_iso_pages"] = raw[mapping["iso_pages"]].map(normalize_text) if mapping.get("iso_pages") else ""
        out["meta_manufacturer_code"] = raw[mapping["manufacturer_code"]].map(normalize_text) if mapping.get("manufacturer_code") else ""
        out["meta_model"] = raw[mapping["model"]].map(normalize_text) if mapping.get("model") else ""
        out["meta_fits_models"] = raw[mapping["fits_models"]].map(normalize_text) if mapping.get("fits_models") else ""
        out = out[out["article_norm"] != ""].reset_index(drop=True)
        return out if not out.empty else _empty_df()

    if suffix == ".csv":
        bio = io.BytesIO(file_bytes)
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
        out = _from_raw(raw, "CSV")
        if out.empty:
            raise ValueError("В файле фото нужны колонки с артикулом и хотя бы с фото или полезными полями.")
        out = out.sort_values(["sheet_priority", "article_norm"]).drop_duplicates(subset=["article_norm"], keep="first").reset_index(drop=True)
        return out[["article", "article_norm", "photo_url", "source_sheet", "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models"]]

    sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    parts: list[pd.DataFrame] = []
    for sheet_name, raw in sheets.items():
        part = _from_raw(raw, sheet_name)
        if not part.empty:
            parts.append(part)

    if not parts:
        raise ValueError("В файле фото нужны колонки с артикулом и хотя бы с фото или полезными полями из Worksheet.")

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.sort_values(["article_norm", "sheet_priority"]).reset_index(drop=True)

    def _first_non_empty(series: pd.Series) -> str:
        for value in series.tolist():
            txt = normalize_text(value)
            if txt:
                return txt
        return ""

    def _best_photo(series: pd.Series) -> str:
        for value in series.tolist():
            txt = normalize_text(value)
            if txt:
                return txt
        return ""

    rows: list[dict[str, str]] = []
    for article_norm, grp in combined.groupby("article_norm", sort=False):
        grp = grp.sort_values(["sheet_priority", "source_sheet"])
        row = {
            "article": _first_non_empty(grp["article"]),
            "article_norm": article_norm,
            "photo_url": _best_photo(grp["photo_url"]),
            "source_sheet": _first_non_empty(grp["source_sheet"]),
            "meta_color": _first_non_empty(grp["meta_color"]),
            "meta_iso_pages": _first_non_empty(grp["meta_iso_pages"]),
            "meta_manufacturer_code": _first_non_empty(grp["meta_manufacturer_code"]),
            "meta_model": _first_non_empty(grp["meta_model"]),
            "meta_fits_models": _first_non_empty(grp["meta_fits_models"]),
        }
        rows.append(row)

    combined = pd.DataFrame(rows)
    return combined[["article", "article_norm", "photo_url", "source_sheet", "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models"]]


def apply_photo_map(df: pd.DataFrame | None, photo_df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None:
        return None
    out = df.copy()
    for col in ["photo_url", "photo_name", "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models"]:
        if col not in out.columns:
            out[col] = ""
    if photo_df is None or photo_df.empty:
        out["photo_name"] = out.get("name", "")
        return out
    lookup = photo_df.set_index("article_norm").to_dict(orient="index")
    def _meta(norm: str, key: str) -> str:
        row = lookup.get(norm, {})
        return normalize_text(row.get(key, ""))
    out["photo_url"] = out["article_norm"].map(lambda x: _meta(x, "photo_url"))
    out["photo_name"] = out["name"]
    out["meta_color"] = out["article_norm"].map(lambda x: _meta(x, "meta_color"))
    out["meta_iso_pages"] = out["article_norm"].map(lambda x: _meta(x, "meta_iso_pages"))
    out["meta_manufacturer_code"] = out["article_norm"].map(lambda x: _meta(x, "meta_manufacturer_code"))
    out["meta_model"] = out["article_norm"].map(lambda x: _meta(x, "meta_model"))
    out["meta_fits_models"] = out["article_norm"].map(lambda x: _meta(x, "meta_fits_models"))
    return out


@st.cache_data(show_spinner=False, ttl=3600, max_entries=6)
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
                "account": normalize_text(r[mapping["account"]]) if mapping.get("account") else "",
            })
        out = pd.DataFrame(rows)
        out["title_norm"] = out["title"].map(contains_text)
        out["title_codes"] = out["title"].map(extract_article_candidates_from_text)
        out["registry_key"] = out.apply(build_avito_registry_key, axis=1)
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
    account_col = find_header_index(AVITO_COLUMN_ALIASES["account"])
    if not title_col:
        raise ValueError("Не удалось определить колонку 'Название объявления' в файле Авито.")

    rows = []
    for r in range(2, ws.max_row + 1):
        ad_display, ad_url = cell_display_and_url(ws.cell(r, ad_id_col)) if ad_id_col else ("", "")
        title_display, title_url = cell_display_and_url(ws.cell(r, title_col))
        explicit_url = normalize_text(ws.cell(r, url_col).value) if url_col else ""
        price_value = normalize_text(ws.cell(r, price_col).value) if price_col else ""
        account_value = normalize_text(ws.cell(r, account_col).value) if account_col else ""
        final_url = explicit_url or title_url or ad_url
        if not ad_display and not title_display:
            continue
        rows.append({
            "ad_id": ad_display,
            "title": title_display,
            "price": price_value,
            "url": final_url,
            "account": account_value,
        })
    out = pd.DataFrame(rows)
    out["title_norm"] = out["title"].map(contains_text)
    out["title_codes"] = out["title"].map(extract_article_candidates_from_text)
    out["registry_key"] = out.apply(build_avito_registry_key, axis=1)
    return out


def get_avito_registry_path() -> Path:
    try:
        return Path(__file__).resolve().with_name("avito_registry.sqlite")
    except Exception:
        return Path.cwd() / "avito_registry.sqlite"


def avito_now_str() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def build_avito_registry_key(row: pd.Series | dict[str, Any]) -> str:
    ad_id = normalize_text(row.get("ad_id", ""))
    if ad_id:
        return f"ad:{ad_id}"
    seed = "|".join([
        normalize_text(row.get("title", "")),
        normalize_text(row.get("url", "")),
    ])
    return "hash:" + hashlib.md5(seed.encode("utf-8", errors="ignore")).hexdigest()


def ensure_avito_registry() -> None:
    path = get_avito_registry_path()
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS avito_registry (
                registry_key TEXT PRIMARY KEY,
                ad_id TEXT,
                title TEXT,
                title_norm TEXT,
                price_raw TEXT,
                url TEXT,
                account TEXT,
                first_seen TEXT,
                last_seen TEXT,
                last_changed_at TEXT,
                previous_price_raw TEXT,
                change_count INTEGER DEFAULT 0,
                status TEXT,
                last_import_name TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def sync_avito_registry(avito_df: pd.DataFrame, import_name: str) -> dict[str, Any]:
    ensure_avito_registry()
    path = get_avito_registry_path()
    now = avito_now_str()
    stats = {"new": 0, "changed": 0, "unchanged": 0, "missing": 0, "total": 0}
    if avito_df is None or avito_df.empty:
        return stats

    work = avito_df.copy()
    work["registry_key"] = work.apply(build_avito_registry_key, axis=1)
    work = work.drop_duplicates(subset=["registry_key"], keep="first").reset_index(drop=True)
    stats["total"] = len(work)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        current_keys = work["registry_key"].tolist()
        placeholders = ",".join(["?"] * len(current_keys)) if current_keys else "''"
        existing = {}
        if current_keys:
            for row in conn.execute(f"SELECT * FROM avito_registry WHERE registry_key IN ({placeholders})", current_keys):
                existing[row["registry_key"]] = dict(row)

        for _, row in work.iterrows():
            key = row["registry_key"]
            payload = {
                "registry_key": key,
                "ad_id": normalize_text(row.get("ad_id", "")),
                "title": normalize_text(row.get("title", "")),
                "title_norm": contains_text(row.get("title", "")),
                "price_raw": normalize_text(row.get("price", "")),
                "url": normalize_text(row.get("url", "")),
                "account": normalize_text(row.get("account", "")),
                "last_import_name": normalize_text(import_name),
            }
            old = existing.get(key)
            if old is None:
                conn.execute(
                    """
                    INSERT INTO avito_registry
                    (registry_key, ad_id, title, title_norm, price_raw, url, account, first_seen, last_seen, last_changed_at, previous_price_raw, change_count, status, last_import_name)
                    VALUES (:registry_key, :ad_id, :title, :title_norm, :price_raw, :url, :account, :first_seen, :last_seen, :last_changed_at, :previous_price_raw, :change_count, :status, :last_import_name)
                    """,
                    {
                        **payload,
                        "first_seen": now,
                        "last_seen": now,
                        "last_changed_at": now,
                        "previous_price_raw": "",
                        "change_count": 0,
                        "status": "active",
                    },
                )
                stats["new"] += 1
            else:
                changed = any([
                    payload["title"] != normalize_text(old.get("title", "")),
                    payload["price_raw"] != normalize_text(old.get("price_raw", "")),
                    payload["url"] != normalize_text(old.get("url", "")),
                    payload["account"] != normalize_text(old.get("account", "")),
                ])
                if changed:
                    conn.execute(
                        """
                        UPDATE avito_registry SET
                            ad_id=:ad_id,
                            title=:title,
                            title_norm=:title_norm,
                            previous_price_raw=:previous_price_raw,
                            price_raw=:price_raw,
                            url=:url,
                            account=:account,
                            last_seen=:last_seen,
                            last_changed_at=:last_changed_at,
                            change_count=:change_count,
                            status='active',
                            last_import_name=:last_import_name
                        WHERE registry_key=:registry_key
                        """,
                        {
                            **payload,
                            "previous_price_raw": normalize_text(old.get("price_raw", "")),
                            "last_seen": now,
                            "last_changed_at": now,
                            "change_count": int(old.get("change_count", 0) or 0) + 1,
                        },
                    )
                    stats["changed"] += 1
                else:
                    conn.execute(
                        """
                        UPDATE avito_registry SET
                            ad_id=:ad_id,
                            title=:title,
                            title_norm=:title_norm,
                            price_raw=:price_raw,
                            url=:url,
                            account=:account,
                            last_seen=:last_seen,
                            status='active',
                            last_import_name=:last_import_name
                        WHERE registry_key=:registry_key
                        """,
                        {**payload, "last_seen": now},
                    )
                    stats["unchanged"] += 1

        if current_keys:
            placeholders = ",".join(["?"] * len(current_keys))
            cur = conn.execute(
                f"UPDATE avito_registry SET status='missing_in_latest_export' WHERE registry_key NOT IN ({placeholders}) AND status='active'",
                current_keys,
            )
            stats["missing"] = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
    finally:
        conn.close()
    return stats


def load_avito_registry_df() -> pd.DataFrame:
    path = get_avito_registry_path()
    if not path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(path)
    try:
        df = pd.read_sql_query("SELECT * FROM avito_registry", conn)
    except Exception:
        conn.close()
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ["ad_id", "title", "price_raw", "url", "account", "status", "first_seen", "last_seen", "last_changed_at", "previous_price_raw", "last_import_name"]:
        if col in df.columns:
            df[col] = df[col].fillna("").map(normalize_text)
    if "title" in df.columns:
        df["title_norm"] = df["title"].map(contains_text)
    return df


def registry_summary_text() -> str:
    df = load_avito_registry_df()
    if df.empty:
        return "Реестр пуст"
    active = int((df.get("status", pd.Series(dtype=object)) == "active").sum()) if "status" in df.columns else len(df)
    changed = int((pd.to_numeric(df.get("change_count", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0).sum()) if "change_count" in df.columns else 0
    return f"В реестре: {len(df)} • активных: {active} • менялись: {changed}"



def get_photo_registry_path() -> Path:
    try:
        return Path(__file__).resolve().with_name("photo_registry.sqlite")
    except Exception:
        return Path.cwd() / "photo_registry.sqlite"


def ensure_photo_registry() -> None:
    path = get_photo_registry_path()
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photo_registry (
                article_norm TEXT PRIMARY KEY,
                article TEXT,
                photo_url TEXT,
                source_sheet TEXT,
                meta_color TEXT,
                meta_iso_pages TEXT,
                meta_manufacturer_code TEXT,
                meta_model TEXT,
                meta_fits_models TEXT,
                first_seen TEXT,
                last_seen TEXT,
                last_changed_at TEXT,
                import_name TEXT,
                change_count INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()


def load_photo_registry_df() -> pd.DataFrame:
    path = get_photo_registry_path()
    if not path.exists():
        return pd.DataFrame()
    with sqlite3.connect(path) as conn:
        df = pd.read_sql_query("SELECT * FROM photo_registry", conn)
    if df.empty:
        return df
    for col in [
        "article", "article_norm", "photo_url", "source_sheet",
        "meta_color", "meta_iso_pages", "meta_manufacturer_code",
        "meta_model", "meta_fits_models", "first_seen", "last_seen",
        "last_changed_at", "import_name",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("").map(normalize_text)
    # Откатили веб-парсер: старые web-fallback записи не подмешиваем в рабочий реестр.
    if "import_name" in df.columns:
        df = df[df["import_name"].fillna("") != "web-fallback"].copy()
    if "source_sheet" in df.columns:
        df = df[~df["source_sheet"].fillna("").str.startswith("web:")].copy()
    return df.reset_index(drop=True)


def photo_registry_summary_text() -> str:
    df = load_photo_registry_df()
    if df.empty:
        return "Реестр фото пуст"
    with_photo = int(df.get("photo_url", pd.Series(dtype=object)).fillna("").map(lambda x: 1 if normalize_text(x) else 0).sum())
    with_meta = int((
        df.get("meta_model", pd.Series(dtype=object)).fillna("").map(bool)
        | df.get("meta_fits_models", pd.Series(dtype=object)).fillna("").map(bool)
        | df.get("meta_color", pd.Series(dtype=object)).fillna("").map(bool)
        | df.get("meta_iso_pages", pd.Series(dtype=object)).fillna("").map(bool)
    ).sum())
    return f"В реестре: {len(df)} • с фото: {with_photo} • с метаданными: {with_meta}"


def sync_photo_registry(photo_df: pd.DataFrame, import_name: str) -> dict[str, Any]:
    ensure_photo_registry()
    path = get_photo_registry_path()
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    work = photo_df.copy() if isinstance(photo_df, pd.DataFrame) else pd.DataFrame()
    if work.empty:
        return {"new": 0, "changed": 0, "unchanged": 0, "total": 0}

    use_cols = [
        "article", "article_norm", "photo_url", "source_sheet",
        "meta_color", "meta_iso_pages", "meta_manufacturer_code",
        "meta_model", "meta_fits_models",
    ]
    for col in use_cols:
        if col not in work.columns:
            work[col] = ""
    work = work[use_cols].copy()
    work = work[work["article_norm"].map(normalize_text) != ""].copy()
    work = work.drop_duplicates(subset=["article_norm"], keep="first").reset_index(drop=True)

    stats = {"new": 0, "changed": 0, "unchanged": 0, "total": len(work)}
    tracked_cols = [
        "article", "photo_url", "source_sheet",
        "meta_color", "meta_iso_pages", "meta_manufacturer_code",
        "meta_model", "meta_fits_models",
    ]

    with sqlite3.connect(path) as conn:
        existing = {}
        keys = work["article_norm"].tolist()
        if keys:
            placeholders = ",".join(["?"] * len(keys))
            for row in conn.execute(f"SELECT * FROM photo_registry WHERE article_norm IN ({placeholders})", keys):
                cols = [d[0] for d in conn.execute("SELECT * FROM photo_registry LIMIT 0").description]
                existing[row[0]] = dict(zip(cols, row))

        for _, rec in work.iterrows():
            key = normalize_text(rec.get("article_norm", ""))
            if not key:
                continue
            payload = {col: normalize_text(rec.get(col, "")) for col in tracked_cols}
            old = existing.get(key)
            if old is None:
                conn.execute(
                    """
                    INSERT INTO photo_registry (
                        article_norm, article, photo_url, source_sheet,
                        meta_color, meta_iso_pages, meta_manufacturer_code, meta_model, meta_fits_models,
                        first_seen, last_seen, last_changed_at, import_name, change_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        key, payload["article"], payload["photo_url"], payload["source_sheet"],
                        payload["meta_color"], payload["meta_iso_pages"], payload["meta_manufacturer_code"], payload["meta_model"], payload["meta_fits_models"],
                        now, now, now, normalize_text(import_name),
                    ),
                )
                stats["new"] += 1
            else:
                changed = any(normalize_text(old.get(col, "")) != payload[col] for col in tracked_cols)
                if changed:
                    change_count = int(old.get("change_count") or 0) + 1
                    conn.execute(
                        """
                        UPDATE photo_registry SET
                            article=?,
                            photo_url=?,
                            source_sheet=?,
                            meta_color=?,
                            meta_iso_pages=?,
                            meta_manufacturer_code=?,
                            meta_model=?,
                            meta_fits_models=?,
                            last_seen=?,
                            last_changed_at=?,
                            import_name=?,
                            change_count=?
                        WHERE article_norm=?
                        """,
                        (
                            payload["article"], payload["photo_url"], payload["source_sheet"],
                            payload["meta_color"], payload["meta_iso_pages"], payload["meta_manufacturer_code"], payload["meta_model"], payload["meta_fits_models"],
                            now, now, normalize_text(import_name), change_count, key,
                        ),
                    )
                    stats["changed"] += 1
                else:
                    conn.execute(
                        "UPDATE photo_registry SET last_seen=?, import_name=? WHERE article_norm=?",
                        (now, normalize_text(import_name), key),
                    )
                    stats["unchanged"] += 1
        conn.commit()
    return stats


def ensure_photo_registry_loaded() -> None:
    if isinstance(st.session_state.get("photo_df"), pd.DataFrame) and not st.session_state.get("photo_df").empty:
        return
    reg = load_photo_registry_df()
    if isinstance(reg, pd.DataFrame) and not reg.empty:
        st.session_state.photo_df = reg[[
            "article", "article_norm", "photo_url", "source_sheet",
            "meta_color", "meta_iso_pages", "meta_manufacturer_code",
            "meta_model", "meta_fits_models",
        ]].copy()
        if normalize_text(st.session_state.get("photo_name", "")) in {"", "ещё не загружен"}:
            st.session_state.photo_name = "из реестра сервера"


def ensure_persisted_source_files_loaded() -> None:
    if (not isinstance(st.session_state.get("comparison_sheets"), dict) or not st.session_state.get("comparison_sheets")) and get_persisted_comparison_file_path().exists():
        load_persisted_comparison_source_into_state()
    if (not isinstance(st.session_state.get("photo_df"), pd.DataFrame) or st.session_state.get("photo_df").empty) and get_persisted_photo_file_path().exists():
        load_persisted_photo_source_into_state()
    if (not isinstance(st.session_state.get("avito_df"), pd.DataFrame) or st.session_state.get("avito_df").empty) and get_persisted_avito_file_path().exists():
        load_persisted_avito_source_into_state()
    if (not isinstance(st.session_state.get("hot_items_df"), pd.DataFrame) or st.session_state.get("hot_items_df").empty) and get_persisted_watchlist_file_path().exists():
        load_persisted_watchlist_source_into_state()






def get_card_override_path() -> Path:
    try:
        return Path(__file__).resolve().with_name("card_overrides.sqlite")
    except NameError:
        return Path.cwd() / "card_overrides.sqlite"


def ensure_card_override_db() -> None:
    path = get_card_override_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS card_overrides (
                sheet_name TEXT NOT NULL,
                article_norm TEXT NOT NULL,
                article TEXT,
                photo_url TEXT,
                name_override TEXT,
                meta_model TEXT,
                meta_manufacturer_code TEXT,
                meta_fits_models TEXT,
                note TEXT,
                updated_at TEXT,
                PRIMARY KEY (sheet_name, article_norm)
            )
            """
        )
        conn.commit()


@st.cache_data(ttl=1800, max_entries=5)
def load_card_overrides_df() -> pd.DataFrame:
    path = get_card_override_path()
    if not path.exists():
        return pd.DataFrame()
    with sqlite3.connect(path) as conn:
        df = pd.read_sql_query("SELECT * FROM card_overrides", conn)
    if df.empty:
        return df
    for col in ["sheet_name", "article_norm", "article", "photo_url", "name_override", "meta_model", "meta_manufacturer_code", "meta_fits_models", "note", "updated_at"]:
        if col in df.columns:
            df[col] = df[col].fillna("").map(normalize_text)
    return df


def clear_card_override_cache() -> None:
    try:
        load_card_overrides_df.clear()
    except Exception:
        pass


def save_card_override(sheet_name: str, article: str, article_norm: str, payload: dict[str, Any]) -> None:
    ensure_card_override_db()
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    clean = {k: normalize_text(v) for k, v in (payload or {}).items()}
    with sqlite3.connect(get_card_override_path()) as conn:
        conn.execute(
            """
            INSERT INTO card_overrides (
                sheet_name, article_norm, article, photo_url, name_override,
                meta_model, meta_manufacturer_code, meta_fits_models, note, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sheet_name, article_norm) DO UPDATE SET
                article=excluded.article,
                photo_url=excluded.photo_url,
                name_override=excluded.name_override,
                meta_model=excluded.meta_model,
                meta_manufacturer_code=excluded.meta_manufacturer_code,
                meta_fits_models=excluded.meta_fits_models,
                note=excluded.note,
                updated_at=excluded.updated_at
            """,
            (
                normalize_text(sheet_name), normalize_text(article_norm), normalize_text(article),
                clean.get("photo_url", ""), clean.get("name_override", ""), clean.get("meta_model", ""),
                clean.get("meta_manufacturer_code", ""), clean.get("meta_fits_models", ""), clean.get("note", ""), now
            ),
        )
        conn.commit()
    clear_card_override_cache()


def delete_card_override(sheet_name: str, article_norm: str) -> None:
    ensure_card_override_db()
    with sqlite3.connect(get_card_override_path()) as conn:
        conn.execute(
            "DELETE FROM card_overrides WHERE sheet_name=? AND article_norm=?",
            (normalize_text(sheet_name), normalize_text(article_norm)),
        )
        conn.commit()
    clear_card_override_cache()



def get_task_registry_path() -> Path:
    try:
        return Path(__file__).resolve().with_name("review_tasks.sqlite")
    except NameError:
        return Path.cwd() / "review_tasks.sqlite"


def ensure_task_registry_db() -> None:
    path = get_task_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_norm TEXT NOT NULL,
                article TEXT,
                sheet_name TEXT,
                name_snapshot TEXT,
                created_at TEXT,
                due_date TEXT,
                status TEXT,
                reason TEXT,
                note TEXT,
                completed_at TEXT,
                source TEXT
            )
            """
        )
        conn.commit()


@st.cache_data(ttl=300, max_entries=4)
def load_task_registry_df() -> pd.DataFrame:
    path = get_task_registry_path()
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "id", "article_norm", "article", "sheet_name", "name_snapshot",
                "created_at", "due_date", "status", "reason", "note", "completed_at", "source"
            ]
        )
    ensure_task_registry_db()
    with sqlite3.connect(path) as conn:
        df = pd.read_sql_query("SELECT * FROM review_tasks ORDER BY id DESC", conn)
    if df.empty:
        return df
    text_cols = [
        "article_norm", "article", "sheet_name", "name_snapshot",
        "created_at", "due_date", "status", "reason", "note", "completed_at", "source"
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").map(normalize_text)
    return df


def clear_task_registry_cache() -> None:
    try:
        load_task_registry_df.clear()
    except Exception:
        pass


def create_review_task(
    article: str,
    article_norm: str,
    sheet_name: str,
    name_snapshot: str,
    due_date: Any,
    reason: str = "",
    note: str = "",
    source: str = "manual_review",
) -> None:
    ensure_task_registry_db()
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    due_txt = ""
    if due_date:
        try:
            if hasattr(due_date, "isoformat"):
                due_txt = due_date.isoformat()
            else:
                due_txt = str(due_date)
        except Exception:
            due_txt = str(due_date)
    with sqlite3.connect(get_task_registry_path()) as conn:
        conn.execute(
            """
            INSERT INTO review_tasks (
                article_norm, article, sheet_name, name_snapshot,
                created_at, due_date, status, reason, note, completed_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalize_text(article_norm),
                normalize_text(article),
                normalize_text(sheet_name),
                normalize_text(name_snapshot),
                now,
                normalize_text(due_txt),
                "NEW",
                normalize_text(reason),
                normalize_text(note),
                "",
                normalize_text(source),
            ),
        )
        conn.commit()
    clear_task_registry_cache()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        if isinstance(value, bool):
            return int(value)
        text_value = str(value).strip()
        if not text_value:
            return int(default)
        return int(float(text_value.replace(",", ".")))
    except Exception:
        return int(default)

def update_review_task_status(task_id: Any, status: str) -> None:
    ensure_task_registry_db()
    task_id = safe_int(task_id, 0)
    if task_id <= 0:
        return
    new_status = normalize_text(status).upper()
    completed_at = ""
    if new_status == "DONE":
        completed_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with sqlite3.connect(get_task_registry_path()) as conn:
        conn.execute(
            "UPDATE review_tasks SET status=?, completed_at=? WHERE id=?",
            (new_status, completed_at, int(task_id)),
        )
        conn.commit()
    clear_task_registry_cache()


def task_effective_status(row: dict[str, Any] | pd.Series) -> str:
    raw_status = normalize_text((row or {}).get("status", "")).upper() if isinstance(row, dict) else normalize_text(row.get("status", "")).upper()
    due_txt = normalize_text((row or {}).get("due_date", "")) if isinstance(row, dict) else normalize_text(row.get("due_date", ""))
    if raw_status in {"DONE", "CANCELLED"}:
        return raw_status
    if due_txt:
        try:
            due_dt = datetime.fromisoformat(due_txt).date()
            if due_dt < datetime.utcnow().date():
                return "OVERDUE"
        except Exception:
            pass
    if raw_status == "ACTIVE":
        return "ACTIVE"
    return "NEW"


def task_status_ru(status: str) -> str:
    mapping = {
        "NEW": "Новая",
        "ACTIVE": "Активная",
        "OVERDUE": "Просрочена",
        "DONE": "Выполнена",
        "CANCELLED": "Отменена",
    }
    return mapping.get(normalize_text(status).upper(), normalize_text(status))


def build_task_view_df(sheet_filter: str | None = None) -> pd.DataFrame:
    df = load_task_registry_df()
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(
            columns=[
                "ID", "Артикул", "Название", "Лист", "Создана", "Срок",
                "Статус", "Причина", "Комментарий", "Источник"
            ]
        )
    work = df.copy()
    work["effective_status"] = work.apply(lambda r: task_effective_status(r), axis=1)
    if sheet_filter and normalize_text(sheet_filter):
        work = work[work.get("sheet_name", pd.Series(dtype=object)).fillna("").map(normalize_text).eq(normalize_text(sheet_filter))]
    work["ID"] = pd.to_numeric(work.get("id", 0), errors="coerce").fillna(0).astype(int)
    work["Артикул"] = work.get("article", "")
    work["Название"] = work.get("name_snapshot", "")
    work["Лист"] = work.get("sheet_name", "")
    work["Создана"] = work.get("created_at", "")
    work["Срок"] = work.get("due_date", "")
    work["Статус"] = work["effective_status"].map(task_status_ru)
    work["Причина"] = work.get("reason", "")
    work["Комментарий"] = work.get("note", "")
    work["Источник"] = work.get("source", "")
    return work[["ID", "Артикул", "Название", "Лист", "Создана", "Срок", "Статус", "Причина", "Комментарий", "Источник"]].copy()


def task_summary_counts() -> dict[str, int]:
    df = load_task_registry_df()
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {"new": 0, "active": 0, "overdue": 0, "done": 0, "open": 0}
    eff = [task_effective_status(r) for _, r in df.iterrows()]
    return {
        "new": sum(1 for x in eff if x == "NEW"),
        "active": sum(1 for x in eff if x == "ACTIVE"),
        "overdue": sum(1 for x in eff if x == "OVERDUE"),
        "done": sum(1 for x in eff if x == "DONE"),
        "open": sum(1 for x in eff if x in {"NEW", "ACTIVE", "OVERDUE"}),
    }


def tasks_summary_text() -> str:
    c = task_summary_counts()
    if c["open"] <= 0:
        return "Задач нет"
    return f"новых: {c['new']} • активных: {c['active']} • просрочено: {c['overdue']}"


def trigger_search_from_task(article: str, sheet_label: str) -> None:
    sheet_label = normalize_text(sheet_label) or "Оригинал"
    mapping = {"Оригинал": "original", "Уценка": "discount", "Совместимые": "compatible"}
    if sheet_label in mapping:
        st.session_state["active_workspace_label"] = sheet_label
        trigger_search_from_article(article, mapping[sheet_label])
    else:
        trigger_search_from_article(article, st.session_state.get("active_workspace_label", "original"))


def apply_task_filters(task_df: pd.DataFrame, status_filter: str, period_filter: str, sheet_filter: str) -> pd.DataFrame:
    if not isinstance(task_df, pd.DataFrame) or task_df.empty:
        return pd.DataFrame(columns=task_df.columns if isinstance(task_df, pd.DataFrame) else [])
    out = task_df.copy()

    if sheet_filter and sheet_filter != "Все листы":
        out = out[out["Лист"].astype(str) == sheet_filter]

    if status_filter == "Новые":
        out = out[out["Статус"].eq("Новая")]
    elif status_filter == "Активные":
        out = out[out["Статус"].eq("Активная")]
    elif status_filter == "Просроченные":
        out = out[out["Статус"].eq("Просрочена")]
    elif status_filter == "Выполненные":
        out = out[out["Статус"].eq("Выполнена")]
    elif status_filter == "Не выполненные":
        out = out[~out["Статус"].isin(["Выполнена", "Отменена"])]

    today = datetime.utcnow().date()
    if period_filter and period_filter != "Все":
        due = pd.to_datetime(out["Срок"], errors="coerce").dt.date
        if period_filter == "Сегодня":
            out = out[due == today]
        elif period_filter == "7 дней":
            out = out[(due.notna()) & (due >= today) & (due <= (today + timedelta(days=7)))]
        elif period_filter == "14 дней":
            out = out[(due.notna()) & (due >= today) & (due <= (today + timedelta(days=14)))]
        elif period_filter == "30 дней":
            out = out[(due.notna()) & (due >= today) & (due <= (today + timedelta(days=30)))]
        elif period_filter == "Просроченные":
            out = out[out["Статус"].eq("Просрочена")]
    return out


def render_tasks_table_ui(task_df: pd.DataFrame, key_prefix: str, default_sheet: str | None = None) -> None:
    if not isinstance(task_df, pd.DataFrame) or task_df.empty:
        st.info("Задач пока нет.")
        return

    filters = st.columns([1.25, 1.25, 1.2])
    status_filter = filters[0].selectbox(
        "Статус",
        ["Все", "Новые", "Активные", "Просроченные", "Выполненные", "Не выполненные"],
        key=f"task_status_filter_{key_prefix}",
        help="Фильтрует задачи по статусу: новые, активные, просроченные, выполненные или все сразу.",
    )
    period_filter = filters[1].selectbox(
        "Период",
        ["Все", "Сегодня", "7 дней", "14 дней", "30 дней", "Просроченные"],
        key=f"task_period_filter_{key_prefix}",
        help="Фильтр по сроку задачи: на сегодня, на ближайшие дни или только просроченные.",
    )
    sheet_options = ["Все листы"] + sorted([x for x in task_df["Лист"].fillna("").astype(str).unique().tolist() if str(x).strip()])
    default_index = sheet_options.index(default_sheet) if default_sheet in sheet_options else 0
    sheet_filter = filters[2].selectbox(
        "Лист",
        sheet_options,
        index=default_index,
        key=f"task_sheet_filter_{key_prefix}",
        help="Ограничивает список задач выбранным листом: Оригинал, Уценка или Совместимые.",
    )

    st.caption(
        "Статус — показывает, на каком этапе задача: новая, активная, просроченная или выполненная. "
        "Период — помогает увидеть срочные и просроченные задачи. "
        "Лист — ограничивает список выбранным разделом comparison."
    )

    filtered = apply_task_filters(task_df, status_filter, period_filter, sheet_filter)
    if filtered.empty:
        st.info("По выбранным фильтрам задач не найдено.")
        return

    st.dataframe(filtered, use_container_width=True, hide_index=True, height=min(520, 120 + len(filtered) * 36))

    labels = []
    row_map = {}
    for _, row in filtered.iterrows():
        label = f"{row['Артикул']} • {row['Статус']} • срок: {row['Срок'] or '—'}"
        labels.append(label)
        row_map[label] = row
    pick_col, b1, b2, b3 = st.columns([4, 1.1, 1.1, 1.1])
    selected = pick_col.selectbox(
        "Открыть или изменить задачу",
        labels,
        key=f"task_selected_{key_prefix}",
        help="Выбери задачу, чтобы открыть карточку товара, отметить её выполненной или вернуть в работу.",
    )
    if not selected:
        return
    row = row_map[selected]
    if b1.button("Открыть", key=f"task_open_{key_prefix}", use_container_width=True):
        trigger_search_from_task(str(row.get("Артикул", "")), str(row.get("Лист", "")))
    if b2.button("Выполнено", key=f"task_done_{key_prefix}", use_container_width=True):
        update_review_task_status(row.get("ID", 0), "DONE")
        st.success(f"Задача по {row.get('Артикул', '')} отмечена как выполненная.")
        st.rerun()
    if b3.button("Вернуть в работу", key=f"task_active_{key_prefix}", use_container_width=True):
        update_review_task_status(row.get("ID", 0), "ACTIVE")
        st.success(f"Задача по {row.get('Артикул', '')} возвращена в работу.")
        st.rerun()


def render_task_center_lazy_panel() -> None:
    active_sheet_name = normalize_text(st.session_state.get("active_workspace_sheet_name", ""))
    header_key = f"crm_show_tasks_{active_sheet_name}" if active_sheet_name else ""
    is_open = bool(st.session_state.get("show_task_center_global", False)) or bool(st.session_state.get(header_key, False))
    if not is_open:
        return
    counts = task_summary_counts()
    task_df = build_task_view_df()
    st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
    render_block_header(
        "Задачи / напоминания",
        "Список задач по карточкам: что проверить, к какому сроку и по какой причине.",
        icon="🔔",
        help_text="Это отдельный слой задач поверх карточек. Задачи не меняют comparison-файл и не пропадают при загрузке нового файла.",
    )
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Открытых задач", counts.get("open", 0))
    m2.metric("Новые", counts.get("new", 0))
    m3.metric("Активные", counts.get("active", 0))
    m4.metric("Просроченные", counts.get("overdue", 0))
    st.caption(
        "Что показывает: задачи на пересмотр карточек и цен. "
        "Как пользоваться: смотри срочные задачи, открывай карточку и после проверки отмечай задачу выполненной."
    )
    render_tasks_table_ui(task_df, "global_tasks")
    st.markdown('</div>', unsafe_allow_html=True)

def apply_card_overrides(df: pd.DataFrame | None, sheet_name: str) -> pd.DataFrame | None:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    overrides = load_card_overrides_df()
    if overrides.empty:
        return df
    sheet_name_norm = normalize_text(sheet_name)
    work = overrides[overrides.get("sheet_name", pd.Series(dtype=object)).fillna("").map(normalize_text).eq(sheet_name_norm)].copy()
    if work.empty:
        return df
    work = work.drop_duplicates(subset=["article_norm"], keep="last")
    by_key = {normalize_text(r["article_norm"]): r for _, r in work.iterrows() if normalize_text(r.get("article_norm", ""))}
    out = df.copy()

    text_cols = [
        "photo_url",
        "source_sheet",
        "name",
        "meta_model",
        "meta_manufacturer_code",
        "meta_fits_models",
        "manual_note",
    ]
    for col in text_cols:
        if col not in out.columns:
            out[col] = ""
        else:
            out[col] = out[col].astype(object)

    for idx, row in out.iterrows():
        key = normalize_text(row.get("article_norm", ""))
        if not key:
            continue
        ov = by_key.get(key)
        if ov is None:
            continue
        photo_url = normalize_text(ov.get("photo_url", ""))
        if photo_url:
            out.at[idx, "photo_url"] = photo_url
            out.at[idx, "source_sheet"] = "manual_override"
        name_override = normalize_text(ov.get("name_override", ""))
        if name_override:
            out.at[idx, "name"] = name_override
        meta_model = normalize_text(ov.get("meta_model", ""))
        if meta_model:
            out.at[idx, "meta_model"] = meta_model
        meta_code = normalize_text(ov.get("meta_manufacturer_code", ""))
        if meta_code:
            out.at[idx, "meta_manufacturer_code"] = meta_code
        meta_fits = normalize_text(ov.get("meta_fits_models", ""))
        if meta_fits:
            out.at[idx, "meta_fits_models"] = meta_fits
        note = normalize_text(ov.get("note", ""))
        if note:
            out.at[idx, "manual_note"] = note
    return out


def trigger_search_from_article(article: str, tab_key: str) -> None:
    query = normalize_query_for_display(article)
    if not query:
        return
    st.session_state[f"search_input_{tab_key}"] = query
    st.session_state[f"submitted_query_{tab_key}"] = query
    st.session_state[f"search_input_widget_pending_{tab_key}"] = query
    st.session_state[f"last_result_{tab_key}"] = None
    st.session_state[f"last_result_sig_{tab_key}"] = None
    st.rerun()


def render_analytics_jump_helper(df: pd.DataFrame | None, tab_key: str, box_key: str) -> None:
    if not isinstance(df, pd.DataFrame) or df.empty or "Артикул" not in df.columns:
        return
    articles = [normalize_text(x) for x in df["Артикул"].tolist() if normalize_text(x)]
    articles = unique_preserve_order(articles)
    if not articles:
        return
    c1, c2 = st.columns([4, 1.2])
    selected_article = c1.selectbox(
        "Открыть позицию в обычном поиске",
        articles,
        key=f"analytics_open_select_{box_key}_{tab_key}",
        help="Быстрый переход из аналитики в обычную карточку товара.",
    )
    if c2.button("Открыть", key=f"analytics_open_btn_{box_key}_{tab_key}", use_container_width=True):
        trigger_search_from_article(selected_article, tab_key)


def render_card_editor_panel(result_df: pd.DataFrame | None, sheet_name: str, tab_key: str) -> None:
    if not isinstance(result_df, pd.DataFrame) or result_df.empty:
        return
    if not st.checkbox("✏️ Редактировать карточку", key=f"show_card_editor_{tab_key}", help="Открывает безопасный редактор карточки. Правки сохраняются поверх файла и не пропадают после новой загрузки comparison."):
        return

    rows = result_df.copy()
    options = []
    option_map = {}
    for _, row in rows.iterrows():
        art = normalize_text(row.get("article", ""))
        key = normalize_text(row.get("article_norm", ""))
        name = normalize_text(row.get("name", ""))
        label = f"{art} — {name[:120]}"
        options.append(label)
        option_map[label] = row

    default_label = options[0] if options else None
    selected_label = st.selectbox("Позиция для редактирования", options, index=0 if default_label else None, key=f"card_editor_select_{tab_key}")
    if not selected_label:
        return
    row = option_map[selected_label]
    art = normalize_text(row.get("article", ""))
    art_norm = normalize_text(row.get("article_norm", ""))
    current_photo = normalize_text(row.get("photo_url", ""))
    current_name = normalize_text(row.get("name", ""))
    current_model = normalize_text(row.get("meta_model", ""))
    current_code = normalize_text(row.get("meta_manufacturer_code", ""))
    current_fits = normalize_text(row.get("meta_fits_models", ""))
    current_note = normalize_text(row.get("manual_note", ""))

    st.caption("Правки сохраняются как ручные overrides и накладываются поверх comparison-файла после каждой новой загрузки.")
    with st.form(f"card_editor_form_{tab_key}_{art_norm}", clear_on_submit=False):
        col1, col2 = st.columns([1.2, 1.8])
        with col1:
            photo_url = st.text_input("Фото (ссылка)", value=current_photo, key=f"card_edit_photo_{tab_key}_{art_norm}")
            if current_photo:
                st.link_button("Открыть текущее фото", current_photo, use_container_width=True)
        with col2:
            name_override = st.text_area("Название", value=current_name, height=90, key=f"card_edit_name_{tab_key}_{art_norm}")
        cmeta1, cmeta2 = st.columns(2)
        meta_model = cmeta1.text_input("Модель", value=current_model, key=f"card_edit_model_{tab_key}_{art_norm}")
        meta_code = cmeta2.text_input("Код производителя", value=current_code, key=f"card_edit_code_{tab_key}_{art_norm}")
        meta_fits = st.text_area("Подходит к моделям", value=current_fits, height=80, key=f"card_edit_fits_{tab_key}_{art_norm}")
        note = st.text_area("Заметка", value=current_note, height=70, key=f"card_edit_note_{tab_key}_{art_norm}")

        st.markdown("### 🔔 Напоминание / задача")
        st.caption("Можно создать задачу на пересмотр позиции через несколько дней. Задача сохранится отдельно и не пропадёт после загрузки нового файла.")
        t1, t2, t3 = st.columns([1.1, 1.4, 1.7])
        create_task_flag = t1.checkbox(
            "Создать задачу",
            key=f"card_edit_make_task_{tab_key}_{art_norm}",
            help="Создаёт напоминание по этой карточке с датой проверки и комментарием.",
        )
        task_due_date = t2.date_input(
            "Когда проверить",
            value=(datetime.utcnow().date() + timedelta(days=14)),
            key=f"card_edit_task_due_{tab_key}_{art_norm}",
        )
        task_reason = t3.selectbox(
            "Причина",
            ["Пересмотреть цену", "Проверить после правки", "Нет продаж", "Проверить фото/карточку", "Проверить спрос", "Другое"],
            key=f"card_edit_task_reason_{tab_key}_{art_norm}",
            help="Коротко описывает, зачем создана задача.",
        )
        task_note = st.text_area(
            "Комментарий к задаче",
            value="",
            height=65,
            key=f"card_edit_task_note_{tab_key}_{art_norm}",
            placeholder="Например: снизили цену, проверить продажи через 14 дней.",
        )

        b1, b2 = st.columns(2)
        save_clicked = b1.form_submit_button("💾 Сохранить карточку", use_container_width=True, type="primary")
        reset_clicked = b2.form_submit_button("↺ Сбросить ручные правки", use_container_width=True)

    if save_clicked:
        save_card_override(
            sheet_name,
            art,
            art_norm,
            {
                "photo_url": photo_url,
                "name_override": name_override,
                "meta_model": meta_model,
                "meta_manufacturer_code": meta_code,
                "meta_fits_models": meta_fits,
                "note": note,
            },
        )
        if create_task_flag:
            create_review_task(
                article=art,
                article_norm=art_norm,
                sheet_name=sheet_name,
                name_snapshot=name_override or current_name,
                due_date=task_due_date,
                reason=task_reason,
                note=task_note or note,
                source="card_editor",
            )
        st.success(f"Карточка {art} сохранена.")
        if create_task_flag:
            st.info(f"Задача по {art} создана до {task_due_date}.")
        st.rerun()

    if reset_clicked:
        delete_card_override(sheet_name, art_norm)
        st.success(f"Ручные правки для {art} сброшены.")
        st.rerun()

def ensure_photo_web_cache_table() -> None:
    path = get_photo_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS photo_web_cache (
                article_norm TEXT PRIMARY KEY,
                article TEXT,
                photo_url TEXT,
                source_page TEXT,
                source_domain TEXT,
                status TEXT,
                checked_at TEXT
            )
            """
        )
        conn.commit()


def get_photo_web_cache(article_norm: str) -> dict[str, Any] | None:
    article_norm = normalize_article(article_norm)
    if not article_norm:
        return None
    ensure_photo_web_cache_table()
    with sqlite3.connect(get_photo_registry_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM photo_web_cache WHERE article_norm=?",
            (article_norm,),
        ).fetchone()
    return dict(row) if row else None


def save_photo_web_cache(article_norm: str, article: str, photo_url: str, source_page: str, source_domain: str, status: str) -> None:
    article_norm = normalize_article(article_norm)
    if not article_norm:
        return
    ensure_photo_web_cache_table()
    with sqlite3.connect(get_photo_registry_path()) as conn:
        conn.execute(
            """
            INSERT INTO photo_web_cache (article_norm, article, photo_url, source_page, source_domain, status, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(article_norm) DO UPDATE SET
                article=excluded.article,
                photo_url=excluded.photo_url,
                source_page=excluded.source_page,
                source_domain=excluded.source_domain,
                status=excluded.status,
                checked_at=excluded.checked_at
            """,
            (article_norm, normalize_text(article), normalize_text(photo_url), normalize_text(source_page), normalize_text(source_domain), normalize_text(status), datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()


def fetch_url_text(url: str, timeout: int = 12) -> str:
    if not normalize_text(url):
        return ""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        "Accept-Language": "ru,en;q=0.9",
    }
    try:
        if requests is not None:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.ok:
                r.encoding = r.encoding or r.apparent_encoding or "utf-8"
                return r.text
            return ""
    except Exception:
        return ""
    return ""


def extract_image_candidates_from_html(html_text: str, page_url: str, article_norm: str = "") -> list[str]:
    if not normalize_text(html_text):
        return []
    attrs = ["content", "src", "data-src", "data-large_image", "data-zoom-image", "data-original", "href"]
    patterns = []
    for attr in attrs:
        patterns.append(rf"{attr}=[\"']([^\"']+)[\"']")
    urls: list[str] = []
    for pat in patterns:
        for match in re.findall(pat, html_text, flags=re.IGNORECASE):
            url = normalize_text(match)
            if not url:
                continue
            abs_url = urljoin(page_url, url)
            low = abs_url.lower()
            if not any(ext in low for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]) and not any(k in low for k in ["image", "images", "product", "uploads", "cache"]):
                continue
            if any(k in low for k in ["logo", "icon", "favicon", "sprite", "placeholder", "blank.gif", "1x1", "captcha"]):
                continue
            urls.append(abs_url)
    seen = set()
    scored: list[tuple[int, str]] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        score = 0
        low = url.lower()
        if article_norm and article_norm.lower() in re.sub(r'[^a-z0-9]', '', low):
            score += 10
        if any(k in low for k in ["product", "goods", "item", "kartrid", "cartridge", "uploads"]):
            score += 3
        if low.endswith((".jpg", ".jpeg", ".png", ".webp")):
            score += 2
        scored.append((score, url))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [u for _, u in scored]


def discover_product_pages(article: str, domain: str) -> list[str]:
    article = normalize_text(article)
    if not article or requests is None:
        return []
    queries = [
        f"site:{domain} {article}",
        f'site:{domain} "{article}"',
    ]
    found: list[str] = []
    for q in queries:
        for search_url in [
            f"https://duckduckgo.com/html/?q={quote_plus(q)}",
            f"https://html.duckduckgo.com/html/?q={quote_plus(q)}",
        ]:
            html_text = fetch_url_text(search_url, timeout=10)
            if not html_text:
                continue
            for raw in re.findall(r"href=[\"']([^\"']+)[\"']", html_text, flags=re.IGNORECASE):
                href = html.unescape(raw)
                href = urljoin(search_url, href)
                if 'duckduckgo.com/l/?uddg=' in href:
                    m = re.search(r'uddg=([^&]+)', href)
                    if m:
                        href = unquote(m.group(1))
                if domain not in href:
                    continue
                if href not in found:
                    found.append(href)
                if len(found) >= FALLBACK_SEARCH_LIMIT:
                    return found
    return found


def discover_photo_url_for_article(article: str) -> tuple[str, str, str]:
    article_norm = normalize_article(article)
    cached = get_photo_web_cache(article_norm)
    if cached and normalize_text(cached.get("status", "")) in {"found", "not_found"}:
        return (normalize_text(cached.get("photo_url", "")), normalize_text(cached.get("source_page", "")), normalize_text(cached.get("source_domain", "")))

    if requests is None:
        save_photo_web_cache(article_norm, article, "", "", "", "not_found")
        return "", "", ""

    for domain in FALLBACK_PHOTO_DOMAINS:
        for page_url in discover_product_pages(article, domain):
            html_text = fetch_url_text(page_url, timeout=12)
            if not html_text:
                continue
            imgs = extract_image_candidates_from_html(html_text, page_url, article_norm=article_norm)
            if imgs:
                best = imgs[0]
                save_photo_web_cache(article_norm, article, best, page_url, domain, "found")
                return best, page_url, domain
    save_photo_web_cache(article_norm, article, "", "", "", "not_found")
    return "", "", ""


def inject_web_photos_into_registry(found_rows: list[dict[str, str]], import_name: str = "web-fallback") -> None:
    if not found_rows:
        return
    payload = pd.DataFrame(found_rows)
    for col in ["article", "article_norm", "photo_url", "source_sheet", "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models"]:
        if col not in payload.columns:
            payload[col] = ""
    sync_photo_registry(payload[["article", "article_norm", "photo_url", "source_sheet", "meta_color", "meta_iso_pages", "meta_manufacturer_code", "meta_model", "meta_fits_models"]], import_name)


def try_fill_missing_photos(df: pd.DataFrame | None, enabled: bool = False, limit: int = 12) -> pd.DataFrame | None:
    if df is None or df.empty or not enabled:
        return df
    work = df.copy()
    missing = work[work.get("photo_url", pd.Series(dtype=object)).fillna("").map(lambda x: not bool(normalize_text(x)))].head(limit)
    if missing.empty:
        return work
    found_rows: list[dict[str, str]] = []
    article_to_url: dict[str, str] = {}
    for _, row in missing.iterrows():
        article = normalize_text(row.get("article", ""))
        article_norm = normalize_article(row.get("article_norm", article))
        if not article_norm:
            continue
        url, source_page, domain = discover_photo_url_for_article(article)
        if url:
            article_to_url[article_norm] = url
            found_rows.append({
                "article": article or article_norm,
                "article_norm": article_norm,
                "photo_url": url,
                "source_sheet": f"web:{domain}",
                "meta_color": normalize_text(row.get("meta_color", "")),
                "meta_iso_pages": normalize_text(row.get("meta_iso_pages", "")),
                "meta_manufacturer_code": normalize_text(row.get("meta_manufacturer_code", "")),
                "meta_model": normalize_text(row.get("meta_model", "")),
                "meta_fits_models": normalize_text(row.get("meta_fits_models", "")),
            })
    if found_rows:
        inject_web_photos_into_registry(found_rows)
        work["photo_url"] = work.apply(lambda r: article_to_url.get(normalize_article(r.get("article_norm", r.get("article", ""))), normalize_text(r.get("photo_url", ""))), axis=1)
        reg_df = load_photo_registry_df()
        if isinstance(reg_df, pd.DataFrame) and not reg_df.empty:
            st.session_state.photo_df = reg_df[[
                "article", "article_norm", "photo_url", "source_sheet",
                "meta_color", "meta_iso_pages", "meta_manufacturer_code",
                "meta_model", "meta_fits_models",
            ]].copy()
    return work

def init_state() -> None:
    defaults = {
        "comparison_sheets": {},
        "comparison_name": "ещё не загружен",
        "comparison_version": "",
        "selected_sheet": "Сравнение",
        "current_df": None,
        "photo_df": None,
        "photo_name": "ещё не загружен",
        "photo_registry_message": "",
        "photo_registry_stats": {},
        "photo_last_sync_sig": "",
        "avito_df": None,
        "avito_name": "ещё не загружен",
        "avito_registry_message": "",
        "avito_registry_stats": {},
        "avito_last_sync_sig": "",
        "hot_items_df": None,
        "hot_items_name": "ещё не загружен",
        "hot_items_last_sync_sig": "",
        "search_input": "",
        "submitted_query": "",
        "last_result": None,
        "price_mode": "-12%",
        "custom_discount": 10.0,
        "round100": True,
        "search_mode": "Умный",
        "template1_footer": DEFAULT_TEMPLATE1_FOOTER,
        "price_patch_input": "",
        "patch_message": "",
        "distributor_threshold": 20.0,
        "distributor_min_qty": 1.0,
        "operation_log": [],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()
ensure_photo_registry_loaded()
ensure_persisted_source_files_loaded()


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
    search_mode = st.session_state.get("search_mode", "Умный")
    tab_specs = [
        ("Сравнение", "original"),
        ("Уценка", "discount"),
        ("Совместимые", "compatible"),
    ]
    for sheet_name, tab_key in tab_specs:
        base_df = sheets.get(sheet_name) if isinstance(sheets, dict) else None
        submitted_key = f"submitted_query_{tab_key}"
        result_key = f"last_result_{tab_key}"
        sig_key = f"last_result_sig_{tab_key}"
        query = normalize_text(st.session_state.get(submitted_key, ""))
        desired_sig = (query, search_mode, sheet_name, st.session_state.get("comparison_version", ""))
        if query and isinstance(base_df, pd.DataFrame):
            st.session_state[result_key] = search_in_df(base_df, query, search_mode, sheet_name=sheet_name)
            st.session_state[sig_key] = desired_sig
        else:
            st.session_state[result_key] = None
            st.session_state[sig_key] = None


def search_in_df(df: pd.DataFrame, query: str, search_mode: str, sheet_name: str = "") -> pd.DataFrame:
    tokens = split_query_parts(query)
    if not tokens:
        return df.iloc[0:0].copy()

    exact_hits = []
    linked_hits = []
    relaxed_hits = []
    contains_hits = []
    seen: set[str] = set()
    relaxed_sheet = sheet_name in {"Уценка", "Совместимые"}

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

        if search_mode in {"Умный", "Артикул + коды из названия", "Артикул + название + бренд"} and token_norm:
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

        # Для Уценки и Совместимых разрешаем более мягкий OEM-поиск:
        # пользователь может ввести оригинальный код, а мы найдём его внутри
        # префиксного артикула/названия (например BS-HPCE505A или CE505A-UCENKA).
        if relaxed_sheet and token_norm:
            relaxed_mask = (
                df["article_norm"].str.contains(re.escape(token_norm), na=False, regex=True)
                | df["search_blob_compact"].str.contains(re.escape(token_norm), na=False, regex=True)
            )
            relaxed = df[relaxed_mask]
            for _, row in relaxed.iterrows():
                key = str(row["article_norm"])
                if key in seen:
                    continue
                seen.add(key)
                row_dict = row.to_dict()
                row_dict["match_type"] = "relaxed"
                row_dict["match_query"] = token
                relaxed_hits.append(row_dict)

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

    rows = exact_hits + linked_hits + relaxed_hits + contains_hits
    if not rows:
        return df.iloc[0:0].copy()
    out = pd.DataFrame(rows)
    rank_map = {"exact": 0, "linked": 1, "relaxed": 2, "contains": 3}
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


def apply_price_updates_to_sheets(sheets: dict[str, pd.DataFrame], updates_text: str) -> tuple[dict[str, pd.DataFrame], str]:
    updates = parse_price_updates(updates_text)
    if not updates:
        return sheets, "Не нашёл строк для правки цен."
    if not isinstance(sheets, dict) or not sheets:
        return sheets, "Сначала загрузите comparison-файл."

    updated_sheets: dict[str, pd.DataFrame] = {}
    total_updated = 0
    hits_by_sheet: list[str] = []
    found_articles: set[str] = set()

    for sheet_name, df in sheets.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            updated_sheets[sheet_name] = df
            continue
        out = df.copy()
        sheet_hits = 0
        if "row_codes" not in out.columns:
            out["row_codes"] = out.apply(lambda row: build_row_compare_codes(row.get("article", ""), row.get("name", "")), axis=1)
        for article_norm, new_price in updates:
            mask = out["row_codes"].apply(lambda codes: article_norm in (codes or []) if isinstance(codes, list) else False)
            if mask.any():
                out.loc[mask, "sale_price"] = float(new_price)
                sheet_hits += int(mask.sum())
                found_articles.add(article_norm)
        updated_sheets[sheet_name] = out
        if sheet_hits:
            total_updated += sheet_hits
            hits_by_sheet.append(f"{sheet_name}: {sheet_hits}")

    missed = [article for article, _ in updates if article not in found_articles]
    msg = f"Обновлено цен: {total_updated}"
    if hits_by_sheet:
        msg += " | По листам: " + "; ".join(hits_by_sheet)
    if missed:
        msg += " | Не найдено: " + ", ".join(missed[:10])
    return updated_sheets, msg


def patch_comparison_workbook_bytes(file_bytes: bytes, updates_text: str) -> tuple[bytes | None, str]:
    updates = parse_price_updates(updates_text)
    if not updates:
        return None, "Не нашёл строк для правки цен."

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    total_updated = 0
    hits_by_sheet: list[str] = []
    found_articles: set[str] = set()

    for ws in wb.worksheets:
        headers = [normalize_text(ws.cell(1, c).value) for c in range(1, ws.max_column + 1)]
        mapping = {key: find_column(headers, aliases) for key, aliases in CATALOG_COLUMN_ALIASES.items()}
        required = ["article", "name", "price"]
        if any(not mapping.get(key) for key in required):
            continue

        def _col_idx(header_value: str | None) -> int | None:
            if not header_value:
                return None
            for idx, header in enumerate(headers, start=1):
                if normalize_text(header) == normalize_text(header_value):
                    return idx
            return None

        article_idx = _col_idx(mapping.get("article"))
        name_idx = _col_idx(mapping.get("name"))
        price_idx = _col_idx(mapping.get("price"))
        if not article_idx or not name_idx or not price_idx:
            continue

        sheet_hits = 0
        for r in range(2, ws.max_row + 1):
            article = normalize_text(ws.cell(r, article_idx).value)
            name = normalize_text(ws.cell(r, name_idx).value)
            row_codes = build_row_compare_codes(article, name)
            matched_price = None
            for article_norm, new_price in updates:
                if article_norm in row_codes:
                    matched_price = float(new_price)
                    found_articles.add(article_norm)
                    break
            if matched_price is None:
                continue
            cell = ws.cell(r, price_idx)
            cell.value = int(matched_price) if float(matched_price).is_integer() else float(matched_price)
            sheet_hits += 1

        if sheet_hits:
            total_updated += sheet_hits
            hits_by_sheet.append(f"{ws.title}: {sheet_hits}")

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)

    missed = [article for article, _ in updates if article not in found_articles]
    msg = f"Обновлено цен: {total_updated}"
    if hits_by_sheet:
        msg += " | По листам: " + "; ".join(hits_by_sheet)
    if missed:
        msg += " | Не найдено: " + ", ".join(missed[:10])
    return bio.read(), msg


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
        price = normalize_merlion_source_price(row, source, price)
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



def translate_watch_action(action: Any, threshold_pct: float = 35.0) -> str:
    raw = normalize_text(action)
    if not raw:
        return ""
    tokens = [t.strip().upper() for t in raw.replace("|", ";").split(";") if t.strip()]
    translated = []
    for token in tokens:
        if token == "BUY":
            translated.append(f"Можно брать (-{int(threshold_pct)}%+)")
        elif token == "RESTOCK":
            translated.append("Пополнить запас")
        elif token == "WATCH":
            translated.append("Наблюдать")
        elif token in {"NO_MATCH", "NO_MATCH_IN_COMPARISON"}:
            translated.append("Нет в сравнении")
        else:
            translated.append(token)
    uniq = []
    for item in translated:
        if item not in uniq:
            uniq.append(item)
    return "; ".join(uniq)

def build_report_df(
    df: pd.DataFrame,
    threshold_percent: float,
    min_qty: float,
    tab_label: str = "",
    hot_lookup: dict[str, list[dict[str, Any]]] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df is None or df.empty:
        return pd.DataFrame()
    hot_lookup = hot_lookup or {}
    for _, row in df.iterrows():
        own_price = safe_float(row.get("sale_price"), 0.0)
        own_qty = safe_float(row.get("free_qty"), 0.0)
        if own_price <= 0:
            continue
        best = get_best_offer(row, min_qty=min_qty)
        if not best:
            continue
        best_price = safe_float(best.get("price"), 0.0)
        best_qty = safe_float(best.get("qty"), 0.0)
        delta = safe_float(best.get("delta"), 0.0)
        delta_pct = safe_float(best.get("delta_percent"), 0.0)
        if best_price <= 0 or delta <= 0 or delta_pct < float(threshold_percent):
            continue

        hot_rec = pick_hot_watch_rec(row, hot_lookup) if hot_lookup else None
        rows.append({
            "Лист": tab_label,
            "Артикул": row.get("article", ""),
            "Товар": row.get("name", ""),
            "Спрос, шт/мес": safe_float((hot_rec or {}).get("sales_per_month"), 0.0) if hot_rec else None,
            "Наша цена": own_price,
            "Наш остаток": own_qty,
            "Лучший поставщик": best.get("source", ""),
            "Цена поставщика": best_price,
            "Остаток поставщика": best_qty,
            "Ниже нашей цены, %": round(delta_pct, 2),
            "Дней запаса": safe_float((hot_rec or {}).get("days_of_cover"), 0.0) if hot_rec else None,
            "Приоритет": safe_float((hot_rec or {}).get("priority_score"), 0.0) if hot_rec else None,
            "Действие": translate_watch_action((hot_rec or {}).get("action_today", ""), threshold_pct=35.0) if hot_rec else "",
            "Разница, руб": delta,
            "Ходовая": "Да" if hot_rec else "",
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out["_sort_priority"] = pd.to_numeric(out.get("Приоритет"), errors="coerce").fillna(-1.0)
    out = out.sort_values(
        ["_sort_priority", "Ниже нашей цены, %", "Разница, руб", "Артикул"],
        ascending=[False, False, False, True],
        kind="stable",
    ).drop(columns=["_sort_priority"]).reset_index(drop=True)
    return out


def report_to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Отчёт")
    bio.seek(0)
    return bio.read()

def build_product_analysis_df(result_df: pd.DataFrame, min_qty: float = 1.0) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if result_df is None or result_df.empty:
        return pd.DataFrame()

    seen: set[str] = set()
    for _, row in result_df.iterrows():
        row_key = str(row.get("article_norm") or normalize_article(row.get("article", "")))
        if row_key in seen:
            continue
        seen.add(row_key)

        best_offer = get_best_offer(row, min_qty=min_qty)
        rows.append({
            "Артикул": str(row.get("article", "") or ""),
            "Название": str(row.get("name", "") or ""),
            "КОЛ.": safe_float(row.get("free_qty", 0), 0.0),
            "тек прод": safe_float(row.get("sale_price", 0), 0.0),
            "дистр": safe_float(best_offer.get("price", 0), 0.0) if best_offer else None,
            "Дистрибьютор": str(best_offer.get("source", "") or "") if best_offer else "",
            "Остаток дистрибьютора": safe_float(best_offer.get("qty", 0), 0.0) if best_offer else None,
        })

    return pd.DataFrame(rows)


def build_product_analysis_workbook_bytes(result_df: pd.DataFrame, min_qty: float = 1.0) -> bytes:
    analysis_df = build_product_analysis_df(result_df, min_qty=min_qty)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Анализ товара"

    headers = [
        "Артикул", "", "КОЛ.", "тек прод", "дистр", "МИ", "ВЦМ", "Ятовары", "Мы на авито",
        "авито мин", "сред. Зак.", "Прод пред", "пред на Авито", "", "% прод", "% Авито"
    ]
    ws.append(headers)

    column_widths = {
        "A": 14, "B": 4, "C": 10, "D": 12, "E": 12, "F": 10, "G": 10, "H": 12,
        "I": 13, "J": 12, "K": 12, "L": 12, "M": 14, "N": 4, "O": 10, "P": 10,
    }
    for col, width in column_widths.items():
        ws.column_dimensions[col].width = width

    header_fill = openpyxl.styles.PatternFill(fill_type="solid", fgColor="D9E2F3")
    thin_gray = openpyxl.styles.Side(style="thin", color="D0D7E2")
    border = openpyxl.styles.Border(left=thin_gray, right=thin_gray, top=thin_gray, bottom=thin_gray)
    header_font = openpyxl.styles.Font(bold=True)
    center = openpyxl.styles.Alignment(horizontal="center", vertical="center")

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.border = border
        cell.alignment = center

    currency_format = '#,##0.00'
    percent_format = '0.00%'

    for excel_row, rec in enumerate(analysis_df.to_dict(orient="records"), start=2):
        ws.cell(excel_row, 1).value = rec.get("Артикул", "")
        ws.cell(excel_row, 3).value = rec.get("КОЛ.", None)
        ws.cell(excel_row, 4).value = rec.get("тек прод", None)
        ws.cell(excel_row, 5).value = rec.get("дистр", None)
        ws.cell(excel_row, 6).value = None
        ws.cell(excel_row, 7).value = None
        ws.cell(excel_row, 8).value = None
        ws.cell(excel_row, 9).value = None
        ws.cell(excel_row, 10).value = None
        ws.cell(excel_row, 11).value = None
        ws.cell(excel_row, 12).value = f'=IF(E{excel_row}="","",E{excel_row}-E{excel_row}*5%)'
        ws.cell(excel_row, 13).value = f'=IF(L{excel_row}="","",L{excel_row}-L{excel_row}*20%)'
        ws.cell(excel_row, 15).value = f'=IF(OR(K{excel_row}="",K{excel_row}=0,L{excel_row}=""),"",L{excel_row}/K{excel_row}-1)'
        ws.cell(excel_row, 16).value = f'=IF(OR(K{excel_row}="",K{excel_row}=0,M{excel_row}=""),"",M{excel_row}/K{excel_row}-1)'

        if rec.get("дистр") not in (None, ""):
            comment_lines = []
            dist_name = normalize_text(rec.get("Дистрибьютор", ""))
            if dist_name:
                comment_lines.append(f"Лучшее предложение: {dist_name}")
            dist_qty = rec.get("Остаток дистрибьютора")
            if dist_qty not in (None, ""):
                comment_lines.append(f"Остаток: {fmt_qty(dist_qty)} шт.")
            if comment_lines:
                ws.cell(excel_row, 5).comment = openpyxl.comments.Comment("\n".join(comment_lines), "ChatGPT")

        for col_idx in [4, 5, 6, 7, 8, 9, 10, 11, 12, 13]:
            ws.cell(excel_row, col_idx).number_format = currency_format
        for col_idx in [15, 16]:
            ws.cell(excel_row, col_idx).number_format = percent_format

    max_row = max(ws.max_row, 2)
    for row in ws.iter_rows(min_row=2, max_row=max_row, min_col=1, max_col=16):
        for cell in row:
            cell.border = border
            if cell.column in (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16):
                cell.alignment = center

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:P{max_row}"

    info = wb.create_sheet("Справка")
    info["A1"] = "Как читать файл"
    info["A1"].font = openpyxl.styles.Font(bold=True, size=12)
    info["A3"] = "Артикул / КОЛ. / тек прод"
    info["B3"] = "Заполняются автоматически из результата поиска и текущего листа comparison-файла."
    info["A4"] = "дистр"
    info["B4"] = "Подставляется лучшая валидная цена поставщика. В комментарии к ячейке есть поставщик и остаток."
    info["A5"] = "МИ / ВЦМ / Ятовары / Мы на авито / авито мин / сред. Зак."
    info["B5"] = "Эти поля вы заполняете вручную перед обсуждением."
    info["A6"] = "Прод пред"
    info["B6"] = "Считается как дистр - 5%."
    info["A7"] = "пред на Авито"
    info["B7"] = "Считается как Прод пред - 20%."
    info["A8"] = "% прод / % Авито"
    info["B8"] = "Считаются относительно среднего закупа."
    info.column_dimensions["A"].width = 26
    info.column_dimensions["B"].width = 90
    info.freeze_panes = "A3"

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()



def build_offer_template(df: pd.DataFrame, query: str, round100: bool, footer_text: str, search_mode: str) -> str:
    result_df = search_in_df(df, query, search_mode, sheet_name=st.session_state.get("selected_sheet", ""))
    if result_df.empty:
        return ""
    lines: list[str] = []
    hashtags: list[str] = []
    for _, row in result_df.iterrows():
        article_head = compose_article_template_label(row)
        if safe_float(row.get("free_qty"), 0.0) > 0:
            avito_raw = safe_float(row.get("sale_price"), 0.0) * (1 - DEFAULT_DISCOUNT_1 / 100)
            cash_raw = avito_raw * 0.90
            avito = round_up_to_100(avito_raw) if round100 else round(avito_raw)
            cash = round_to_nearest_100(cash_raw) if round100 else round(cash_raw)
            lines.append(f"{article_head} --- {fmt_price(avito)} руб. - Авито / {fmt_price(cash)} руб. за наличный расчет")
        else:
            lines.append(f"{article_head} --- продан")
        hashtags.append(f"#{normalize_article(row['article'])}")

    shared_lines = build_template_shared_lines(result_df)
    footer = [normalize_text(x) for x in str(footer_text).splitlines() if normalize_text(x)]
    out_lines: list[str] = []
    out_lines.extend(lines)
    if shared_lines:
        out_lines.append("")
        out_lines.extend(shared_lines)
    if footer:
        out_lines.extend(footer)
    if hashtags:
        out_lines.append(",".join(unique_preserve_order(hashtags)))
    return "\n".join(out_lines)


def build_selected_price_template(df: pd.DataFrame, query: str, price_mode: str, round100: bool, custom_discount: float, search_mode: str) -> str:
    result_df = search_in_df(df, query, search_mode, sheet_name=st.session_state.get("selected_sheet", ""))
    if result_df.empty:
        return ""
    parts = []
    for _, row in result_df.iterrows():
        if safe_float(row.get("free_qty"), 0.0) <= 0:
            continue
        selected_price = get_selected_price_raw(row, price_mode, round100, custom_discount)
        parts.append(f"{normalize_text(row['name'])} --- {fmt_price(selected_price)} руб.")
    return "\n\n".join(parts)




def build_offer_template_from_result_df(result_df: pd.DataFrame, round100: bool, footer_text: str) -> str:
    if result_df is None or result_df.empty:
        return ""
    lines: list[str] = []
    hashtags: list[str] = []
    for _, row in result_df.iterrows():
        article_head = compose_article_template_label(row)
        if safe_float(row.get("free_qty"), 0.0) > 0:
            avito_raw = safe_float(row.get("sale_price"), 0.0) * (1 - DEFAULT_DISCOUNT_1 / 100)
            cash_raw = avito_raw * 0.90
            avito = round_up_to_100(avito_raw) if round100 else round(avito_raw)
            cash = round_to_nearest_100(cash_raw) if round100 else round(cash_raw)
            lines.append(f"{article_head} --- {fmt_price(avito)} руб. - Авито / {fmt_price(cash)} руб. за наличный расчет")
        else:
            lines.append(f"{article_head} --- продан")
        hashtags.append(f"#{normalize_article(row['article'])}")

    shared_lines = build_template_shared_lines(result_df)
    footer = [normalize_text(x) for x in str(footer_text).splitlines() if normalize_text(x)]
    out_lines: list[str] = []
    out_lines.extend(lines)
    if shared_lines:
        out_lines.append("")
        out_lines.extend(shared_lines)
    if footer:
        out_lines.extend(footer)
    if hashtags:
        out_lines.append(",".join(unique_preserve_order(hashtags)))
    return "\n".join(out_lines)


def build_selected_price_template_from_result_df(result_df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float) -> str:
    if result_df is None or result_df.empty:
        return ""
    parts: list[str] = []
    for _, row in result_df.iterrows():
        if safe_float(row.get("free_qty"), 0.0) <= 0:
            continue
        selected_price = get_selected_price_raw(row, price_mode, round100, custom_discount)
        parts.append(f"{normalize_text(row['name'])} --- {fmt_price(selected_price)} руб.")
    return "\n\n".join(parts)


def find_avito_ads(avito_df: pd.DataFrame, result_df: pd.DataFrame) -> pd.DataFrame:
    registry_df = load_avito_registry_df()
    if (avito_df is None or avito_df.empty) and registry_df.empty:
        return pd.DataFrame()
    if result_df is None or result_df.empty:
        return pd.DataFrame()

    tokens: list[str] = []
    raw_tokens: list[str] = []
    for _, row in result_df.iterrows():
        row_codes = row.get("row_codes", [])
        if isinstance(row_codes, list) and row_codes:
            tokens.extend(unique_norm_codes(row_codes))
        else:
            tokens.extend(build_row_compare_codes(row.get("article", ""), row.get("name", "")))
        article_raw = normalize_text(row.get("article", ""))
        if article_raw:
            raw_tokens.append(article_raw)
        for code in extract_article_candidates_from_text(row.get("name", "")):
            raw_tokens.append(code)
    tokens = unique_preserve_order([normalize_article(x) for x in tokens if normalize_article(x)])
    raw_tokens = unique_preserve_order([normalize_article(x) for x in raw_tokens if normalize_article(x)])
    if not tokens and not raw_tokens:
        return pd.DataFrame()

    base_df = avito_df.copy() if isinstance(avito_df, pd.DataFrame) and not avito_df.empty else registry_df.copy()
    if base_df.empty:
        return pd.DataFrame()
    if "title_norm" not in base_df.columns:
        base_df["title_norm"] = base_df["title"].map(contains_text)
    if "title_codes" not in base_df.columns:
        base_df["title_codes"] = base_df["title"].map(extract_article_candidates_from_text)
    if "registry_key" not in base_df.columns:
        base_df["registry_key"] = base_df.apply(build_avito_registry_key, axis=1)

    matches = []
    for _, row in base_df.iterrows():
        title_codes = unique_norm_codes(row.get("title_codes", []) if isinstance(row.get("title_codes", []), list) else extract_article_candidates_from_text(row.get("title", "")))
        title_compact = compact_text(row.get("title", ""))
        exact_hits = [t for t in tokens if t in title_codes]
        substring_hits = [t for t in tokens if t and t in title_compact and t not in exact_hits]
        raw_hits = [t for t in raw_tokens if t and t in title_compact and t not in exact_hits and t not in substring_hits]
        if exact_hits or substring_hits or raw_hits:
            item = row.to_dict()
            item["matched_tokens"] = ", ".join(unique_preserve_order(exact_hits + substring_hits + raw_hits))
            item["match_score"] = len(exact_hits) * 100 + len(substring_hits) * 10 + len(raw_hits)
            item["match_kind"] = "точное" if exact_hits else ("связанное" if substring_hits else "по названию")
            matches.append(item)
    if not matches:
        return pd.DataFrame()

    out = pd.DataFrame(matches)
    out = out.sort_values(["match_score", "registry_key"], ascending=[False, True]).drop_duplicates(subset=["registry_key"], keep="first").reset_index(drop=True)
    if not registry_df.empty:
        reg = registry_df.copy()
        if "registry_key" not in reg.columns:
            reg["registry_key"] = reg.apply(build_avito_registry_key, axis=1)
        reg_cols = [c for c in ["registry_key", "first_seen", "last_seen", "last_changed_at", "previous_price_raw", "change_count", "status", "account", "last_import_name"] if c in reg.columns]
        out = out.merge(reg[reg_cols], on="registry_key", how="left", suffixes=("", "_reg"))
        if "account_reg" in out.columns:
            out["account"] = out["account"].where(out["account"].astype(str).str.len() > 0, out["account_reg"])
            out = out.drop(columns=["account_reg"])
    return out


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



def render_results_table(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float, distributor_map: Optional[dict[str, dict[str, Any]]] = None, show_photos: bool = True) -> None:
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

        hot_html = ""
        manual_note_html = ""
        manual_note = normalize_text(row.get("manual_note", ""))
        if manual_note:
            manual_note_html = f"<div class='manual-note'>{html.escape(manual_note)}</div>"
        if bool(row.get("hot_flag", False)):
            abc = normalize_text(row.get("hot_abc_class", "")).upper()
            sales_pm = safe_float(row.get("hot_sales_per_month"), 0.0)
            action_today = normalize_text(row.get("hot_action_today", "")).upper()
            buy_signal = normalize_text(row.get("hot_buy_signal", "")).upper()

            demand_map = {
                "A": "Спрос высокий",
                "B": "Спрос хороший",
                "C": "Спрос умеренный",
            }
            primary = "🔥 Товар ходовой"
            note_parts = []
            if abc:
                note_parts.append(demand_map.get(abc, f"Класс {abc}"))
            if sales_pm > 0:
                note_parts.append(f"≈ {sales_pm:.1f} шт/мес")
            note = " • ".join(note_parts)

            action_text, _hot_help = hot_supplier_note(row, best, threshold_pct=35.0)
            action_html = f"<div class='hot-sub-badge'>{html.escape(action_text)}</div>" if action_text else ""
            hot_html = f"<div class='hot-badge'>{html.escape(primary)}</div>{('<div class="hot-meta">' + html.escape(note) + '</div>') if note else ''}{action_html}"

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

        photo_url = normalize_text(row.get("photo_url", "")) if show_photos else ""
        if show_photos and photo_url:
            photo_html = f"""
            <a href="{html.escape(photo_url, quote=True)}" target="_blank" class="photo-wrap">
              <img src="{html.escape(photo_url, quote=True)}" class="result-photo" loading="lazy" onerror="this.style.display='none'; this.parentNode.innerHTML='<div class=&quot;photo-empty photo-empty-small&quot;>нет фото</div>';">
            </a>
            """
        elif show_photos:
            photo_html = "<div class='photo-empty photo-empty-small'>нет фото</div>"
        else:
            photo_html = ""

        item_photo_html = f"<div class='item-photo'>{photo_html}</div>" if show_photos else ""

        free_qty = safe_float(row.get("free_qty"), 0.0)
        total_qty = safe_float(row.get("total_qty"), free_qty)
        transit_qty = safe_float(row.get("transit_qty"), 0.0)
        stock_html = f"""
        <div class='stock-main'>{fmt_qty(free_qty)}</div>
        <div class='stock-sub'>Свободно: {fmt_qty(free_qty)}</div>
        <div class='stock-sub'>Всего: {fmt_qty(total_qty)}</div>
        <div class='stock-sub'>Транзит: {fmt_qty(transit_qty)}</div>
        """

        rows_html.append(
            f"""
            <tr>
              <td class='item-col'>
                <div class='item-wrap'>
                  {item_photo_html}
                  <div class='item-main'>
                    <div class='item-top'><span class='article-pill'>{html.escape(str(row['article']))}</span></div>
                    <div class='name-cell'>{html.escape(str(row['name']))}</div>
                    {badge_html}
                    {hot_html}
                    {manual_note_html}
                  </div>
                </div>
              </td>
              <td class='stock-cell'>{stock_html}</td>
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
      .manual-note {{ margin-top:8px; font-size:12px; color:#475569; background:#f8fafc; border:1px dashed #cbd5e1; padding:6px 8px; border-radius:10px; max-width:560px; }}
      .sale-col {{ font-weight:800; white-space:nowrap; }}
      .selected-col {{ background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%); border-left:1px solid #c7d7ff; border-right:1px solid #c7d7ff; font-weight:900; color:#315efb; white-space:nowrap; }}
      .compare-col {{ min-width:230px; }}
      .stock-cell {{ min-width:110px; }}
      .stock-main {{ font-weight:900; font-size:18px; color:#0f172a; line-height:1.05; margin-bottom:6px; }}
      .stock-sub {{ font-size:12px; color:#64748b; line-height:1.4; }}
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


def render_all_prices_block(result_df: pd.DataFrame, min_qty: float, price_mode: str, round100: bool, custom_discount: float, widget_key_prefix: str = "main") -> None:
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
        file_name=f"moy_tovar_all_prices_{widget_key_prefix}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"download_all_prices_{widget_key_prefix}",
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
    hot_count = 0
    hot_buy_count = 0
    if isinstance(result_df, pd.DataFrame) and not result_df.empty and "hot_flag" in result_df.columns:
        hot_count = int(result_df["hot_flag"].fillna(False).map(bool).sum())
        if "hot_buy_signal" in result_df.columns:
            hot_buy_count = int(result_df["hot_buy_signal"].fillna("").map(normalize_text).str.upper().eq("BUY").sum())
    hot_label = "Ходовые в поиске"
    hot_note = "Watchlist не совпал с результатами"
    hot_help = "Товар ходовой → товар хорошо продавался за выбранный период"
    if hot_count:
        hot_label = "Товар ходовой"
        if hot_buy_count > 0:
            hot_note = "Сейчас можно брать"
            hot_help += "\nСейчас можно брать → лучший поставщик сейчас минимум на 35% дешевле нашей цены"
        else:
            hot_note = "Сейчас брать невыгодно"
            hot_help += "\nСейчас брать невыгодно → нет поставщика с ценой минимум на 35% ниже нашей цены"
    cards = [
        ("🔎", "Найдено позиций", str(found_count), "Сколько строк вошло в текущий поиск", ""),
        ("💚", "Есть цена лучше", str(better_rows), "Сколько позиций реально дешевле у поставщиков", ""),
        ("📈", "Средняя выгода", (f"{avg_gain:.1f}%" if gains else "—"), "Считается приложением, не берётся из готовых колонок Excel", ""),
        ("🔥", hot_label, str(hot_count), hot_note, hot_help),
        ("🧩", "Источников найдено", str(len(source_pairs)), ", ".join([x["source"] for x in source_pairs]) if source_pairs else "Нет колонок источников", ""),
    ]
    html_parts = []
    for icon, label, value, note, help_text in cards:
        help_html = f"<span class='insight-help' title='{html.escape(help_text)}'>?</span>" if help_text else ""
        html_parts.append(
            f"<div class='insight-card'><div class='insight-top'><span class='insight-icon'>{icon}</span><span class='insight-label'>{label}</span>{help_html}</div><div class='insight-value'>{value}</div><div class='insight-note'>{note}</div></div>"
        )
    html_cards = "".join(html_parts)
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
        account = normalize_text(row.get("account", ""))
        left, right = st.columns([6, 2])
        with left:
            st.markdown(f"**{html.escape(title)}**", unsafe_allow_html=True)
        with right:
            if account:
                st.markdown(
                    f"<div style='text-align:right;'><span style='display:inline-block;padding:6px 10px;border-radius:999px;background:#eef4ff;border:1px solid #d8e5ff;color:#315efb;font-weight:800;font-size:12px;'>{html.escape(account)}</span></div>",
                    unsafe_allow_html=True,
                )
        meta = []
        if normalize_text(row.get("ad_id", "")):
            meta.append(f"ID: {normalize_text(row.get('ad_id'))}")
        if normalize_text(row.get("price", "")):
            meta.append(f"Цена: {normalize_text(row.get('price'))}")
        if normalize_text(row.get("matched_tokens", "")):
            meta.append(f"Совпадения: {normalize_text(row.get('matched_tokens'))}")
        if meta:
            st.caption(" • ".join(meta))
        hist = []
        if normalize_text(row.get("first_seen", "")):
            hist.append(f"Впервые: {normalize_text(row.get('first_seen'))}")
        if normalize_text(row.get("last_seen", "")):
            hist.append(f"Последняя выгрузка: {normalize_text(row.get('last_seen'))}")
        if normalize_text(row.get("last_changed_at", "")):
            hist.append(f"Изменение: {normalize_text(row.get('last_changed_at'))}")
        if normalize_text(row.get("previous_price_raw", "")) and normalize_text(row.get("price", "")) and normalize_text(row.get("previous_price_raw", "")) != normalize_text(row.get("price", "")):
            hist.append(f"Было: {normalize_text(row.get('previous_price_raw'))}")
        if hist:
            st.caption(" • ".join(hist))
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
        background: #ffffff !important;
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        caret-color: #0f172a !important;
        border: 1px solid #d7e2f2 !important;
        border-radius: 16px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85), 0 4px 10px rgba(2,8,23,.08) !important;
    }
    [data-testid="stSidebar"] .stTextArea textarea::placeholder,
    [data-testid="stSidebar"] .stTextInput input::placeholder,
    [data-testid="stSidebar"] .stNumberInput input::placeholder,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea::placeholder {
        color: #7b8798 !important;
        -webkit-text-fill-color: #7b8798 !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stNumberInput button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="button"],
    [data-testid="stSidebar"] .stNumberInput svg {
        background: #eef3ff !important;
        color: #315efb !important;
        fill: #315efb !important;
        stroke: #315efb !important;
        border-color: #d7e2f2 !important;
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
        background: #ffffff !important;
        border-radius: 16px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85), 0 4px 10px rgba(2,8,23,.08) !important;
        border: 1px solid #d7e2f2 !important;
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
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"] > div > button,
    [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"] > div > button {
        background: #eef3ff !important;
        color: #315efb !important;
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
        background: #ffffff !important;
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        border-color: #d7e2f2 !important;
        border: 1px solid #d7e2f2 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85), 0 4px 10px rgba(2,8,23,.08) !important;
        border-radius: 16px !important;
    }
    [data-testid="stSidebar"] input::placeholder,
    [data-testid="stSidebar"] textarea::placeholder {
        color: #7b8798 !important;
        -webkit-text-fill-color: #7b8798 !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] svg,
    [data-testid="stSidebar"] div[role="combobox"] svg {
        fill: #6b7ea6 !important;
        color: #6b7ea6 !important;
    }
    [data-testid="stSidebar"] [data-baseweb="input"] button,
    [data-testid="stSidebar"] [data-baseweb="base-input"] button,
    [data-testid="stSidebar"] .stNumberInput button {
        background: #eef3ff !important;
        color: #315efb !important;
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stTextArea textarea,
    [data-testid="stSidebar"] [data-baseweb="textarea"] textarea {
        padding: 12px 14px !important;
        line-height: 1.55 !important;
    }
    /* ultra-specific fix for lingering white textarea/select backgrounds in sidebar */
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] textarea,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] textarea:focus,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] textarea:hover,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] .stTextArea textarea,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] .stTextArea textarea:focus,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] .stTextArea textarea:hover,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] [data-baseweb="textarea"],
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] [data-baseweb="textarea"] > div,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] [data-baseweb="textarea"] textarea,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] [data-baseweb="select"],
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] div[role="combobox"] {
        background-color: #ffffff !important;
        background: #ffffff !important;
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        border: 1px solid #d7e2f2 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85), 0 4px 10px rgba(2,8,23,.08) !important;
        border-radius: 16px !important;
    }
    [data-testid="stSidebar"] section[data-testid="stSidebarContent"] textarea::placeholder {
        color: #7b8798 !important;
        -webkit-text-fill-color: #7b8798 !important;
        opacity: 1 !important;
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
    .insight-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 14px 0 16px 0; }
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
    if REQUESTS_IMPORT_ERROR:
        st.caption("requests не установлен: веб-функции отключены")

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Comparison-файл", "📘", "Главный файл приложения. Содержит листы Сравнение, Уценка, Совместимые. Можно хранить последний файл на сервере в /data.")
    uploaded = st.file_uploader("Загрузить comparison-файл", type=["xlsx", "xlsm"], label_visibility="collapsed")
    if uploaded is not None:
        try:
            comp_bytes = uploaded.getvalue()
            save_uploaded_source_file(get_persisted_comparison_file_path(), comp_bytes, uploaded.name)
            clear_loader_caches()
            st.session_state.comparison_sheets = load_comparison_workbook(uploaded.name, comp_bytes)
            st.session_state.comparison_name = uploaded.name + " • сохранён в /data"
            st.session_state.comparison_version = datetime.utcnow().isoformat()
            available = list(st.session_state.comparison_sheets.keys())
            if available and st.session_state.selected_sheet not in available:
                st.session_state.selected_sheet = available[0]
            rebuild_current_df()
            refresh_all_search_results()
            log_operation(f"Обновлён comparison-файл: {uploaded.name}", "success")
        except Exception as exc:
            log_operation(f"Ошибка comparison-файла: {exc}", "warning")
            st.error(f"Ошибка файла: {exc}")
    else:
        if not (isinstance(st.session_state.get("comparison_sheets"), dict) and st.session_state.get("comparison_sheets")):
            load_persisted_comparison_source_into_state()
    st.markdown(f'<div class="sidebar-status">Файл: {html.escape(st.session_state.get("comparison_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">Файл на сервере: {html.escape(str(get_persisted_comparison_file_path()))}</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-mini">Рабочие разделы переключаются сверху: <b>Оригинал</b>, <b>Уценка</b>, <b>Совместимые</b>. Рендерится только активный раздел — это быстрее.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Фото товаров", "🖼️", "Файл с артикулами и ссылками на фото. Можно держать его в реестре на сервере и дозагружать только новинки/изменения.")
    photo_uploaded = st.file_uploader("Загрузить файл фото", type=["xlsx", "xls", "xlsm", "csv"], key="photo_uploader", label_visibility="collapsed")
    if photo_uploaded is not None:
        try:
            photo_bytes = photo_uploaded.getvalue()
            save_uploaded_source_file(get_persisted_photo_file_path(), photo_bytes, photo_uploaded.name)
            clear_loader_caches()
            photo_sig = hashlib.md5(photo_bytes).hexdigest()
            loaded_photo_df = load_photo_map_file(photo_uploaded.name, photo_bytes)
            if st.session_state.get("photo_last_sync_sig", "") != photo_sig:
                photo_stats = sync_photo_registry(loaded_photo_df, photo_uploaded.name)
                st.session_state.photo_registry_stats = photo_stats
                st.session_state.photo_registry_message = (
                    f"Синхронизация фото: новых {photo_stats.get('new', 0)}, обновлённых {photo_stats.get('changed', 0)}, без изменений {photo_stats.get('unchanged', 0)}. Исходник сохранён в /data"
                )
                st.session_state.photo_last_sync_sig = photo_sig
            reg_df = load_photo_registry_df()
            if isinstance(reg_df, pd.DataFrame) and not reg_df.empty:
                st.session_state.photo_df = reg_df[[
                    "article", "article_norm", "photo_url", "source_sheet",
                    "meta_color", "meta_iso_pages", "meta_manufacturer_code",
                    "meta_model", "meta_fits_models",
                ]].copy()
            else:
                st.session_state.photo_df = loaded_photo_df
            st.session_state.photo_name = photo_uploaded.name + " • сохранён в /data"
            rebuild_current_df()
            refresh_all_search_results()
            log_operation(f"Обновлён каталог фото: {photo_uploaded.name}", "success")
        except Exception as exc:
            log_operation(f"Ошибка файла фото: {exc}", "warning")
            st.error(f"Ошибка файла фото: {exc}")
    else:
        if not isinstance(st.session_state.get("photo_df"), pd.DataFrame):
            if not load_persisted_photo_source_into_state():
                ensure_photo_registry_loaded()
    st.markdown(f'<div class="sidebar-status">Фото: {html.escape(st.session_state.get("photo_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">Файл на сервере: {html.escape(str(get_persisted_photo_file_path()))}</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-mini">Автопоиск фото на внешних сайтах отключён. Используются только данные из Каталога расходки и локального реестра.</div>', unsafe_allow_html=True)
    if st.session_state.get("photo_registry_message"):
        st.markdown(f'<div class="sidebar-mini">{html.escape(st.session_state.get("photo_registry_message"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">{html.escape(photo_registry_summary_text())}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Авито", "🛒", "Загруженный файл Авито помогает найти действующие объявления. Параллельно ведём локальный реестр: новые объявления добавляются, изменившиеся обновляются.")
    avito_uploaded = st.file_uploader("Загрузить файл Авито", type=["xlsx", "xlsm", "csv"], key="avito_uploader", label_visibility="collapsed")
    if avito_uploaded is not None:
        try:
            avito_bytes = avito_uploaded.getvalue()
            save_uploaded_source_file(get_persisted_avito_file_path(), avito_bytes, avito_uploaded.name)
            clear_loader_caches()
            avito_sig = hashlib.md5(avito_bytes).hexdigest()
            st.session_state.avito_df = load_avito_file(avito_uploaded.name, avito_bytes)
            st.session_state.avito_name = avito_uploaded.name + " • сохранён в /data"
            if st.session_state.get("avito_last_sync_sig", "") != avito_sig:
                sync_stats = sync_avito_registry(st.session_state.avito_df, avito_uploaded.name)
                st.session_state.avito_registry_stats = sync_stats
                st.session_state.avito_registry_message = (
                    f"Синхронизация: новых {sync_stats.get('new', 0)}, изменённых {sync_stats.get('changed', 0)}, без изменений {sync_stats.get('unchanged', 0)}. Исходник сохранён в /data"
                )
                st.session_state.avito_last_sync_sig = avito_sig
            log_operation(f"Обновлён файл Авито: {avito_uploaded.name}", "success")
        except Exception as exc:
            log_operation(f"Ошибка файла Авито: {exc}", "warning")
            st.error(f"Ошибка файла Авито: {exc}")
    else:
        if not isinstance(st.session_state.get("avito_df"), pd.DataFrame):
            load_persisted_avito_source_into_state()
    st.markdown(f'<div class="sidebar-status">Авито: {html.escape(st.session_state.get("avito_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">Файл на сервере: {html.escape(str(get_persisted_avito_file_path()))}</div>', unsafe_allow_html=True)
    if st.session_state.get("avito_registry_message"):
        st.markdown(f'<div class="sidebar-mini">{html.escape(st.session_state.get("avito_registry_message"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">{html.escape(registry_summary_text())}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Ходовые позиции", "🔥", "Watchlist с продажами за период. Можно хранить на сервере и подсвечивать ходовые позиции прямо в результатах поиска.")
    hot_uploaded = st.file_uploader("Загрузить watchlist", type=["xlsx", "xls", "csv"], key="hot_items_uploader", label_visibility="collapsed")
    if hot_uploaded is not None:
        try:
            hot_bytes = hot_uploaded.getvalue()
            save_uploaded_source_file(get_persisted_watchlist_file_path(), hot_bytes, hot_uploaded.name)
            clear_loader_caches()
            st.session_state.hot_items_df = load_hot_watchlist_file(hot_uploaded.name, hot_bytes)
            st.session_state.hot_items_name = hot_uploaded.name + " • сохранён в /data"
            st.session_state.hot_items_last_sync_sig = hashlib.md5(hot_bytes).hexdigest()
            log_operation(f"Обновлён watchlist ходовых: {hot_uploaded.name}", "success")
        except Exception as exc:
            log_operation(f"Ошибка файла ходовых: {exc}", "warning")
            st.error(f"Ошибка watchlist: {exc}")
    else:
        if not isinstance(st.session_state.get("hot_items_df"), pd.DataFrame):
            load_persisted_watchlist_source_into_state()
    st.markdown(f'<div class="sidebar-status">Watchlist: {html.escape(st.session_state.get("hot_items_name", "ещё не загружен"))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">Файл на сервере: {html.escape(str(get_persisted_watchlist_file_path()))}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-mini">{html.escape(hot_watchlist_summary_text())}</div>', unsafe_allow_html=True)
    hot_df_state = st.session_state.get("hot_items_df")
    hot_buy_total = 0
    if isinstance(hot_df_state, pd.DataFrame) and not hot_df_state.empty:
        hot_buy_total = int(hot_df_state.get("buy_signal_30pct", pd.Series(dtype=object)).fillna("").map(normalize_text).str.upper().eq("BUY").sum())
    st.checkbox(f"Показать таблицу «можно брать» ({hot_buy_total})", key="show_hot_buy_watchlist_table", help="Лениво открывает таблицу только по ходовым позициям со статусом BUY. Пока чекбокс выключен, таблица не строится.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Отчёт и цены", "📊", "Порог выгоды и минимальный остаток для пересчёта лучшей цены.")
    st.number_input("Порог отчёта, %", min_value=0.0, max_value=95.0, step=1.0, key="distributor_threshold")
    st.number_input("Мин. остаток у поставщика", min_value=1.0, max_value=999999.0, step=1.0, key="distributor_min_qty")
    st.markdown('<div class="sidebar-mini">Колонки Мин. у конкурентов / Разница из Excel не используются. Всё считаем заново прямо в приложении.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Быстрая правка цен", "✏️", "Меняет Наша цена по всем листам comparison-файла. Полезно для локальной проверки без правки исходного Excel.")
    st.text_area("Правка цен", key="price_patch_input", height=110, label_visibility="collapsed", placeholder="CE278A 8900\nCF364A - 29700")
    if st.button("Править цены в файле", use_container_width=True):
        sheets_state = st.session_state.get("comparison_sheets")
        persisted_path = get_persisted_comparison_file_path()
        original_name = read_persisted_original_name(persisted_path, persisted_path.name) if persisted_path.exists() else st.session_state.get("comparison_name", "comparison_latest.xlsx")
        if persisted_path.exists():
            try:
                before_snapshot = build_price_snapshot_for_updates(st.session_state.get("comparison_sheets"), st.session_state.price_patch_input)
                updated_bytes, patch_message = patch_comparison_workbook_bytes(persisted_path.read_bytes(), st.session_state.price_patch_input)
                if updated_bytes is not None:
                    save_uploaded_source_file(persisted_path, updated_bytes, original_name.replace(" • из /data", "").replace(" • сохранён в /data", ""))
                    clear_loader_caches()
                    st.session_state.comparison_sheets = load_comparison_workbook(original_name, updated_bytes)
                    st.session_state.comparison_name = original_name + " • из /data"
                    st.session_state.comparison_version = datetime.utcnow().isoformat()
                    rebuild_current_df()
                    refresh_all_search_results()
                    after_snapshot = build_price_snapshot_for_updates(st.session_state.get("comparison_sheets"), st.session_state.price_patch_input)
                    history_logged = log_price_patch_history_diff(before_snapshot, after_snapshot, source="manual", note="Быстрая правка цен")
                    if history_logged:
                        patch_message += f" | История: {history_logged}"
                st.session_state.patch_message = patch_message
                log_operation(f"Быстрая правка цен: {patch_message}", "success")
            except Exception as exc:
                st.session_state.patch_message = f"Ошибка правки файла: {exc}"
                log_operation(f"Ошибка быстрой правки цен: {exc}", "warning")
        elif isinstance(sheets_state, dict) and sheets_state:
            before_snapshot = build_price_snapshot_for_updates(sheets_state, st.session_state.price_patch_input)
            updated_sheets, patch_message = apply_price_updates_to_sheets(sheets_state, st.session_state.price_patch_input)
            st.session_state.comparison_sheets = updated_sheets
            st.session_state.comparison_version = datetime.utcnow().isoformat()
            rebuild_current_df()
            after_snapshot = build_price_snapshot_for_updates(updated_sheets, st.session_state.price_patch_input)
            history_logged = log_price_patch_history_diff(before_snapshot, after_snapshot, source="manual", note="Быстрая правка цен")
            if history_logged:
                patch_message += f" | История: {history_logged}"
            st.session_state.patch_message = patch_message
            refresh_all_search_results()
            log_operation(f"Быстрая правка цен: {patch_message}", "success")
        else:
            st.session_state.patch_message = "Сначала загрузите comparison-файл."
            log_operation("Быстрая правка цен: comparison-файл не загружен", "warning")
    if st.session_state.get("patch_message"):
        st.markdown(f'<div class="sidebar-mini">{html.escape(st.session_state.patch_message)}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="sidebar-mini">Прайс сохраняется локально. После правок цены не пропадут до загрузки нового файла.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Настройки", "⚙️", "Управляет режимом поиска, главной ценой, пользовательской скидкой и округлением.")
    st.radio("Режим поиска", ["Только артикул", "Умный", "Артикул + название + бренд"], key="search_mode")
    st.radio("Какая цена главная", ["-12%", "-20%", "Своя скидка"], key="price_mode")
    st.number_input("Своя скидка, %", min_value=0.0, max_value=99.0, step=1.0, key="custom_discount")
    st.checkbox("Округлять вверх до 100", key="round100")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-card">', unsafe_allow_html=True)
    render_sidebar_card_header("Текст шаблона 1", "🧾", "Этот текст добавляется один раз в конце шаблона 1.")
    st.markdown('<div class="sidebar-card-note">Этот текст добавляется один раз в конце шаблона 1. Хэштеги по артикулам подставляются автоматически.</div>', unsafe_allow_html=True)
    st.text_area("Текст шаблона 1", key="template1_footer", height=170, label_visibility="collapsed")
    st.markdown('<div class="sidebar-mini">Текст сохраняется локально и останется до следующего изменения.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    render_operation_log_sidebar()

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
<div class="brand-box"><div class="logo">📦</div><div><div class="brand-title">{APP_TITLE}</div><div class="brand-sub">Один comparison-файл • поиск • фото • пересчёт цен поставщиков • {APP_VERSION}</div></div></div>
<div class="stat-box"><div class="stat-cap">Файл</div><div class="stat-val">{html.escape(comparison_name)}</div></div>
<div class="stat-box"><div class="stat-cap">Вкладок</div><div class="stat-val">{loaded_sheet_count if loaded_sheet_count else '—'}</div></div>
<div class="stat-box"><div class="stat-cap">Всего строк</div><div class="stat-val">{rows_count}</div></div>
</div></div>
""", unsafe_allow_html=True)





def get_price_patch_history_path() -> Path:
    try:
        return Path(__file__).resolve().with_name("price_patch_history.sqlite")
    except Exception:
        return Path.cwd() / "price_patch_history.sqlite"


def ensure_price_patch_history() -> None:
    path = get_price_patch_history_path()
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_patch_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                changed_at TEXT,
                article_norm TEXT,
                article TEXT,
                sheet_name TEXT,
                old_price REAL,
                new_price REAL,
                change_source TEXT,
                note TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def load_price_patch_history_df(limit: int = 200) -> pd.DataFrame:
    path = get_price_patch_history_path()
    if not path.exists():
        return pd.DataFrame()
    ensure_price_patch_history()
    conn = sqlite3.connect(path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM price_patch_history ORDER BY id DESC LIMIT ?",
            conn,
            params=(int(limit),),
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ["changed_at", "article_norm", "article", "sheet_name", "change_source", "note"]:
        if col in df.columns:
            df[col] = df[col].fillna("").map(normalize_text)
    return df


def build_price_snapshot_for_updates(sheets: dict[str, pd.DataFrame] | None, updates_text: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    updates = parse_price_updates(updates_text)
    snapshot: dict[tuple[str, str, str], dict[str, Any]] = {}
    if not updates or not isinstance(sheets, dict):
        return snapshot
    target_codes = [code for code, _ in updates if normalize_article(code)]
    if not target_codes:
        return snapshot
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        for target in target_codes:
            mask = df["article_norm"].eq(target)
            if "row_codes" in df.columns:
                row_mask = df["row_codes"].apply(lambda codes: target in (codes or []) if isinstance(codes, list) else False)
                mask = mask | row_mask
            matched = df[mask]
            if matched.empty:
                continue
            for _, row in matched.iterrows():
                article_txt = normalize_text(row.get("article", ""))
                key = (target, sheet_name, normalize_article(article_txt) or target)
                snapshot[key] = {
                    "article_norm": target,
                    "article": article_txt or target,
                    "sheet_name": sheet_name,
                    "price": safe_float(row.get("sale_price"), 0.0),
                }
    return snapshot


def log_price_patch_history_diff(before: dict[tuple[str, str, str], dict[str, Any]], after: dict[tuple[str, str, str], dict[str, Any]], source: str = "manual", note: str = "") -> int:
    ensure_price_patch_history()
    rows: list[tuple[str, str, str, str, float, float, str, str]] = []
    changed_at = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    all_keys = set(before.keys()) | set(after.keys())
    for key in all_keys:
        prev = before.get(key)
        cur = after.get(key)
        old_price = safe_float((prev or {}).get("price"), 0.0)
        new_price = safe_float((cur or {}).get("price"), 0.0)
        if abs(old_price - new_price) < 1e-9:
            continue
        row = cur or prev or {}
        rows.append(
            (
                changed_at,
                normalize_text(row.get("article_norm", key[0])),
                normalize_text(row.get("article", key[2])),
                normalize_text(row.get("sheet_name", key[1])),
                old_price,
                new_price,
                normalize_text(source),
                normalize_text(note),
            )
        )
    if not rows:
        return 0
    conn = sqlite3.connect(get_price_patch_history_path())
    try:
        conn.executemany(
            """
            INSERT INTO price_patch_history (
                changed_at, article_norm, article, sheet_name, old_price, new_price, change_source, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def infer_brand_from_product_name(name: object) -> str:
    text = contains_text(name)
    if not text:
        return ""
    patterns = [
        ("KONICA MINOLTA", "Konica Minolta"),
        ("KONICA-MINOLTA", "Konica Minolta"),
        ("KYOCERA", "Kyocera"),
        ("BROTHER", "Brother"),
        ("CANON", "Canon"),
        ("PANTUM", "Pantum"),
        ("XEROX", "Xerox"),
        ("LEXMARK", "Lexmark"),
        ("RICOH", "Ricoh"),
        ("SAMSUNG", "Samsung"),
        ("SHARP", "Sharp"),
        ("PANASONIC", "Panasonic"),
        ("EPSON", "Epson"),
        ("OKI", "OKI"),
        ("HP", "HP"),
    ]
    for marker, label in patterns:
        if marker in text:
            return label
    return ""


def parse_dt_safe(value: object) -> Optional[datetime]:
    txt = normalize_text(value)
    if not txt:
        return None
    for parser in (datetime.fromisoformat,):
        try:
            return parser(txt)
        except Exception:
            continue
    return None


def combine_avito_sources(avito_df: pd.DataFrame | None, registry_df: pd.DataFrame | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if isinstance(avito_df, pd.DataFrame) and not avito_df.empty:
        cur = avito_df.copy()
        if "registry_key" not in cur.columns:
            cur["registry_key"] = cur.apply(build_avito_registry_key, axis=1)
        if "title_codes" not in cur.columns:
            cur["title_codes"] = cur.get("title", "").map(extract_article_candidates_from_text)
        if "title_norm" not in cur.columns:
            cur["title_norm"] = cur.get("title", "").map(contains_text)
        frames.append(cur)
    if isinstance(registry_df, pd.DataFrame) and not registry_df.empty:
        reg = registry_df.copy()
        if "registry_key" not in reg.columns:
            reg["registry_key"] = reg.apply(build_avito_registry_key, axis=1)
        if "title_codes" not in reg.columns:
            reg["title_codes"] = reg.get("title", "").map(extract_article_candidates_from_text)
        if "title_norm" not in reg.columns:
            reg["title_norm"] = reg.get("title", "").map(contains_text)
        frames.append(reg)
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = merged.drop_duplicates(subset=["registry_key"], keep="first").reset_index(drop=True)
    for col in ["ad_id", "title", "price", "price_raw", "url", "account", "status", "last_changed_at", "last_seen", "first_seen"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").map(normalize_text)
    return merged


def build_avito_code_index(avito_df: pd.DataFrame | None) -> tuple[pd.DataFrame, dict[str, list[dict[str, Any]]]]:
    if avito_df is None or avito_df.empty:
        return pd.DataFrame(), {}
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    records = avito_df.to_dict(orient="records")
    for rec in records:
        codes = rec.get("title_codes", []) or []
        codes = unique_norm_codes(codes)
        for code in codes:
            index[code].append(rec)
    return avito_df, index


def match_avito_candidates_for_codes(index: dict[str, list[dict[str, Any]]], codes: list[str]) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    seen: set[str] = set()
    for code in unique_norm_codes(codes):
        for rec in index.get(code, []):
            key = normalize_text(rec.get("registry_key")) or normalize_text(rec.get("ad_id")) or normalize_text(rec.get("title"))
            if key in seen:
                continue
            seen.add(key)
            hits.append(rec)
    return hits


def safe_days_since(dt_value: object) -> Optional[int]:
    dt = parse_dt_safe(dt_value)
    if not dt:
        return None
    try:
        delta = datetime.now() - dt
        return max(int(delta.days), 0)
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=900, max_entries=6)
def build_operational_analytics_bundle(
    sheet_df: pd.DataFrame,
    photo_df: pd.DataFrame | None,
    avito_df: pd.DataFrame | None,
    avito_registry_df: pd.DataFrame | None,
    min_qty: float,
    sheet_name: str,
) -> dict[str, Any]:
    enriched = apply_photo_map(sheet_df, photo_df)
    if enriched is None or enriched.empty:
        return {}
    merged_avito = combine_avito_sources(avito_df, avito_registry_df)
    _, avito_index = build_avito_code_index(merged_avito)

    rows_meta: list[dict[str, Any]] = []
    source_counter: Counter[str] = Counter()
    task_counts = Counter()
    account_rows: list[dict[str, Any]] = []

    for _, row in enriched.iterrows():
        article = normalize_text(row.get("article", ""))
        article_norm = normalize_article(article)
        name = normalize_text(row.get("name", ""))
        own_price = safe_float(row.get("sale_price"), 0.0)
        own_qty = parse_qty_generic(row.get("free_qty"))
        codes = row.get("row_codes", []) or build_row_compare_codes(article, name)
        matched_ads = match_avito_candidates_for_codes(avito_index, codes)
        ad_count = len(matched_ads)
        accounts = unique_text_values([rec.get("account", "") for rec in matched_ads])
        best_offer = get_best_offer(row, min_qty=min_qty)
        better_market = bool(best_offer and safe_float(best_offer.get("price"), 0.0) > 0 and safe_float(best_offer.get("price"), 0.0) < own_price)
        delta_rub = own_price - safe_float((best_offer or {}).get("price"), 0.0) if better_market else 0.0
        delta_pct = ((delta_rub / own_price) * 100.0) if better_market and own_price > 0 else 0.0
        priority_score = round(max(delta_pct, 0.0) * max(own_qty, 0.0) * max(ad_count, 1), 2)
        photo_url = normalize_text(row.get("photo_url", ""))
        has_photo = bool(photo_url)
        has_model = bool(normalize_text(row.get("meta_model", "")))
        has_fits = bool(normalize_text(row.get("meta_fits_models", "")))
        template_ok = has_photo and (has_model or has_fits)
        days_since_change_candidates = [safe_days_since(rec.get("last_changed_at", "")) for rec in matched_ads]
        days_since_change_candidates = [x for x in days_since_change_candidates if x is not None]
        days_since_change = min(days_since_change_candidates) if days_since_change_candidates else None
        stale = bool(days_since_change is not None and days_since_change >= 30)
        weak_ad = ad_count > 0 and not template_ok
        reasons: list[str] = []
        if own_qty > 0 and better_market:
            reasons.append("дорого")
            task_counts["price_review"] += 1
        if own_qty > 0 and not has_photo:
            reasons.append("нет фото")
            task_counts["no_photo"] += 1
        if own_qty > 0 and ad_count == 0:
            reasons.append("нет объявления")
            task_counts["no_avito"] += 1
        elif own_qty > 0 and weak_ad:
            reasons.append("слабое объявление")
            task_counts["weak_avito"] += 1
        if own_qty > 0 and stale:
            reasons.append("давно не обновлялось")
            task_counts["stale"] += 1
        if better_market and best_offer:
            source_counter[normalize_text(best_offer.get("source", ""))] += 1
        brand_guess = infer_brand_from_product_name(name)

        row_meta = {
            "Лист": sheet_name,
            "Артикул": article,
            "Название": name,
            "Бренд": brand_guess,
            "Наша цена": own_price,
            "Наш остаток": own_qty,
            "Лучшая цена дистрибьютора": safe_float((best_offer or {}).get("price"), 0.0) if best_offer else None,
            "Лучший поставщик": normalize_text((best_offer or {}).get("source", "")) if best_offer else "",
            "Остаток дистрибьютора": safe_float((best_offer or {}).get("qty"), 0.0) if best_offer else None,
            "Разница, руб": delta_rub if better_market else None,
            "Разница, %": round(delta_pct, 2) if better_market else None,
            "Приоритет": priority_score if better_market else 0.0,
            "Фото": "Да" if has_photo else "Нет",
            "Шаблон": "OK" if template_ok else "Пустой",
            "Модель": "Да" if has_model else "Нет",
            "Подходит к моделям": "Да" if has_fits else "Нет",
            "Объявлений Авито": ad_count,
            "Аккаунты Авито": ", ".join(accounts),
            "Последнее изменение, дней": days_since_change if days_since_change is not None else "",
            "Причины": ", ".join(reasons),
            "article_norm": article_norm,
            "family": split_article_family_suffix(article_norm)[0],
            "color": simplify_template_color(normalize_text(row.get("meta_color", "")) or extract_color_from_text(name)),
        }
        rows_meta.append(row_meta)
        for account in accounts:
            account_rows.append(
                {
                    "Аккаунт": account,
                    "Артикул": article,
                    "Бренд": brand_guess,
                    "Лист": sheet_name,
                    "Есть фото": has_photo,
                    "Есть объявление": ad_count > 0,
                    "Лучше рынка": better_market,
                    "Шаблон OK": template_ok,
                }
            )

    meta_df = pd.DataFrame(rows_meta)
    top_df = meta_df[meta_df["Разница, %"].notna()].copy() if not meta_df.empty else pd.DataFrame()
    if not top_df.empty:
        top_df = top_df.sort_values(["Приоритет", "Разница, %", "Наш остаток"], ascending=[False, False, False]).reset_index(drop=True)

    action_df = meta_df[meta_df["Причины"].map(bool)].copy() if not meta_df.empty else pd.DataFrame()
    if not action_df.empty:
        action_df = action_df.sort_values(["Наш остаток", "Разница, %"], ascending=[False, False], na_position="last").reset_index(drop=True)

    quality = {
        "with_photo": int((meta_df["Фото"] == "Да").sum()) if not meta_df.empty else 0,
        "without_photo": int((meta_df["Фото"] == "Нет").sum()) if not meta_df.empty else 0,
        "template_ok": int((meta_df["Шаблон"] == "OK").sum()) if not meta_df.empty else 0,
        "without_model_or_fits": int(((meta_df["Модель"] == "Нет") | (meta_df["Подходит к моделям"] == "Нет")).sum()) if not meta_df.empty else 0,
        "in_price_not_in_avito": int((meta_df["Объявлений Авито"] == 0).sum()) if not meta_df.empty else 0,
        "in_avito_not_in_stock": int(((meta_df["Объявлений Авито"] > 0) & (pd.to_numeric(meta_df["Наш остаток"], errors="coerce").fillna(0) <= 0)).sum()) if not meta_df.empty else 0,
    }
    quality_df = pd.DataFrame(
        [
            {"Показатель": "С фото", "Количество": quality["with_photo"]},
            {"Показатель": "Без фото", "Количество": quality["without_photo"]},
            {"Показатель": "Шаблон заполнен", "Количество": quality["template_ok"]},
            {"Показатель": "Нет модели / подходит к моделям", "Количество": quality["without_model_or_fits"]},
            {"Показатель": "Есть в прайсе, но нет в Avito", "Количество": quality["in_price_not_in_avito"]},
            {"Показатель": "Есть в Avito, но нет в наличии", "Количество": quality["in_avito_not_in_stock"]},
        ]
    )

    account_df = pd.DataFrame(account_rows)
    if not account_df.empty:
        account_summary = account_df.groupby("Аккаунт", dropna=False).agg(
            Позиций=("Артикул", "count"),
            Без_фото=("Есть фото", lambda s: int((~pd.Series(s)).sum())),
            Дороже_рынка=("Лучше рынка", lambda s: int(pd.Series(s).sum())),
            Слабый_шаблон=("Шаблон OK", lambda s: int((~pd.Series(s)).sum())),
        ).reset_index().rename(columns={"Без_фото": "Без фото", "Дороже_рынка": "Дороже рынка", "Слабый_шаблон": "Слабый шаблон"})
    else:
        account_summary = pd.DataFrame(columns=["Аккаунт", "Позиций", "Без фото", "Дороже рынка", "Слабый шаблон"])

    series_rows: list[dict[str, Any]] = []
    if not meta_df.empty:
        for family, grp in meta_df.groupby("family"):
            if not normalize_text(family) or len(grp) < 2:
                continue
            colors = unique_text_values(grp["color"].tolist())
            issue_count = int((grp["Фото"] == "Нет").sum()) + int((grp["Объявлений Авито"] == 0).sum()) + int(grp["Разница, %"].notna().sum())
            canonical = [c for c in colors if c in {"чёрный", "черный", "голубой", "пурпурный", "жёлтый", "желтый"}]
            missing = []
            palette = ["чёрный", "голубой", "пурпурный", "жёлтый"]
            normalized_colors = {c.replace("ё", "е") for c in canonical}
            if normalized_colors:
                for color in palette:
                    if color.replace("ё", "е") not in normalized_colors:
                        missing.append(color)
            series_rows.append(
                {
                    "Серия": family,
                    "Позиций": len(grp),
                    "Цвета": ", ".join(colors),
                    "Не хватает": ", ".join(missing),
                    "Без фото": int((grp["Фото"] == "Нет").sum()),
                    "Без Avito": int((grp["Объявлений Авито"] == 0).sum()),
                    "Дороже рынка": int(grp["Разница, %"].notna().sum()),
                    "Проблем в серии": issue_count,
                }
            )
    series_df = pd.DataFrame(series_rows)
    if not series_df.empty:
        series_df = series_df.sort_values(["Проблем в серии", "Позиций"], ascending=[False, False]).reset_index(drop=True)

    source_df = pd.DataFrame(
        [{"Источник": source, "Сколько раз лучший": count} for source, count in source_counter.most_common()]
    )

    patch_history_df = load_price_patch_history_df(limit=50)

    tasks_df = pd.DataFrame(
        [
            {"Задача": "Пересмотреть по цене", "Количество": int(task_counts.get("price_review", 0))},
            {"Задача": "Добавить фото", "Количество": int(task_counts.get("no_photo", 0))},
            {"Задача": "Доработать/добавить Avito", "Количество": int(task_counts.get("no_avito", 0) + task_counts.get("weak_avito", 0))},
            {"Задача": "Проверить давно не обновлявшиеся", "Количество": int(task_counts.get("stale", 0))},
            {"Задача": "Проверить неполные серии", "Количество": int((series_df["Не хватает"].map(bool)).sum()) if not series_df.empty else 0},
        ]
    )

    return {
        "meta_df": meta_df,
        "top_df": top_df,
        "action_df": action_df,
        "quality_df": quality_df,
        "account_df": account_summary,
        "series_df": series_df,
        "source_df": source_df,
        "patch_history_df": patch_history_df,
        "tasks_df": tasks_df,
        "quality": quality,
    }


def analytics_bundle_to_excel_bytes(bundle: dict[str, Any]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for key, sheet_name in [
            ("top_df", "Приоритет цены"),
            ("action_df", "Что делать"),
            ("account_df", "Аккаунты Avito"),
            ("quality_df", "Качество карточек"),
            ("series_df", "Серии"),
            ("source_df", "Источники"),
            ("patch_history_df", "История правок"),
            ("tasks_df", "Сегодня"),
        ]:
            df = bundle.get(key)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    bio.seek(0)
    return bio.read()


def render_operational_analytics_block(sheet_df: pd.DataFrame, photo_df: pd.DataFrame | None, avito_df: pd.DataFrame | None, min_qty: float, sheet_name: str, tab_key: str) -> None:
    registry_df = load_avito_registry_df()
    bundle = build_operational_analytics_bundle(sheet_df, photo_df, avito_df, registry_df, min_qty, sheet_name)
    if not bundle:
        st.info("Для аналитики нет данных.")
        return
    quality = bundle.get("quality", {})
    tasks_df = bundle.get("tasks_df", pd.DataFrame())
    top_df = bundle.get("top_df", pd.DataFrame())
    action_df = bundle.get("action_df", pd.DataFrame())
    account_df = bundle.get("account_df", pd.DataFrame())
    quality_df = bundle.get("quality_df", pd.DataFrame())
    series_df = bundle.get("series_df", pd.DataFrame())
    source_df = bundle.get("source_df", pd.DataFrame())
    patch_history_df = bundle.get("patch_history_df", pd.DataFrame())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Без фото", int(quality.get("without_photo", 0)))
    m2.metric("Нет в Avito", int(quality.get("in_price_not_in_avito", 0)))
    m3.metric("Дороже рынка", int(len(top_df) if isinstance(top_df, pd.DataFrame) else 0))
    m4.metric("История ручных правок", int(len(patch_history_df) if isinstance(patch_history_df, pd.DataFrame) else 0))

    render_info_banner(
        "Что делать сегодня",
        "Сначала посмотри приоритет на пересмотр цены, потом проблемные позиции с фото/Avito, потом качество карточек и серии.",
        icon="🧭",
        chips=[f"лист: {sheet_name}", "ленивый расчёт", "ядро поиска не затрагивается"],
        tone="green",
    )

    if isinstance(tasks_df, pd.DataFrame) and not tasks_df.empty:
        st.dataframe(tasks_df, use_container_width=True, hide_index=True)

    with st.expander("1. Приоритет на пересмотр цены ❔", expanded=False):
        st.caption(
            "Что показывает: где мы дороже рынка и что стоит пересмотреть в первую очередь. "
            "Как считается приоритет: Разница, % × Наш остаток × Количество связанных объявлений Avito (минимум 1). "
            "Как пользоваться: чем выше приоритет, тем полезнее сначала проверить именно эту позицию."
        )
        if isinstance(top_df, pd.DataFrame) and not top_df.empty:
            view = top_df[["Артикул", "Название", "Наша цена", "Лучшая цена дистрибьютора", "Лучший поставщик", "Разница, руб", "Разница, %", "Наш остаток", "Остаток дистрибьютора", "Приоритет"]].head(100)
            st.dataframe(view, use_container_width=True, hide_index=True)
            render_analytics_jump_helper(view, tab_key, "top")
        else:
            st.caption("На текущем листе нет позиций, где рынок дешевле нас.")

    with st.expander("2. Что лежит и требует вмешательства ❔", expanded=False):
        st.caption(
            "Что показывает: позиции, где уже есть проблема или задача. "
            "Причины могут быть такие: дорого, нет фото, нет объявления, слабое объявление, давно не обновлялось. "
            "Как пользоваться: это твой список того, что нужно доработать в первую очередь."
        )
        if isinstance(action_df, pd.DataFrame) and not action_df.empty:
            view = action_df[["Артикул", "Название", "Наш остаток", "Причины", "Объявлений Авито", "Фото", "Шаблон", "Лучший поставщик", "Разница, %"]].head(150)
            st.dataframe(view, use_container_width=True, hide_index=True)
            render_analytics_jump_helper(view, tab_key, "action")
        else:
            st.caption("Явных проблемных позиций на текущем листе не найдено.")

    with st.expander("3. Аналитика по аккаунтам Avito ❔", expanded=False):
        st.caption(
            "Что показывает: как позиции распределены по аккаунтам Avito. "
            "Можно понять, на каком аккаунте больше карточек, где больше позиций без фото и где больше дорогих позиций. "
            "Как пользоваться: помогает управлять аккаунтами не на глаз, а по факту."
        )
        if isinstance(account_df, pd.DataFrame) and not account_df.empty:
            st.dataframe(account_df, use_container_width=True, hide_index=True)
        else:
            st.caption("В Avito пока нет данных по аккаунтам для этого листа.")

    with st.expander("4. Покрытие качества карточек ❔", expanded=False):
        st.caption(
            "Что показывает: насколько хорошо заполнены карточки товаров на текущем листе. "
            "Здесь видно, сколько позиций с фото, без фото, с моделью, с полем 'подходит к моделям', есть ли Avito и есть ли остаток. "
            "Как пользоваться: это быстрый контроль качества карточек."
        )
        st.dataframe(quality_df, use_container_width=True, hide_index=True)

    with st.expander("5. Серийная аналитика ❔", expanded=False):
        st.caption(
            "Что показывает: серии товаров, где есть несколько связанных артикулов, цветов или вариантов. "
            "Помогает увидеть неполные серии, серии без фото, без Avito или с ценовыми перекосами. "
            "Как пользоваться: полезно, чтобы не продавать серию обрывками."
        )
        if isinstance(series_df, pd.DataFrame) and not series_df.empty:
            series_view = series_df.head(100)
            st.dataframe(series_view, use_container_width=True, hide_index=True)
            render_analytics_jump_helper(series_view, tab_key, "series")
        else:
            st.caption("На текущем листе не найдено серий, требующих отдельной сводки.")

    with st.expander("6. История ручных правок ❔", expanded=False):
        st.caption(
            "Что показывает: журнал ручных изменений цен. "
            "Видно когда меняли, какой артикул, на каком листе, было / стало и источник изменения. "
            "Как пользоваться: помогает понять, что менялось руками, а что пришло из нового comparison-файла."
        )
        if isinstance(patch_history_df, pd.DataFrame) and not patch_history_df.empty:
            st.dataframe(patch_history_df[["changed_at", "article", "sheet_name", "old_price", "new_price", "change_source", "note"]], use_container_width=True, hide_index=True)
        else:
            st.caption("История ручных правок пока пустая.")

    with st.expander("7. Надёжность источников ❔", expanded=False):
        st.caption(
            "Что показывает: как часто каждый поставщик оказывался лучшим по цене на текущем листе. "
            "Это не гарантия, а практический индикатор, кого стоит чаще мониторить. "
            "Как пользоваться: помогает понять, какие источники чаще всего дают хорошие цены."
        )
        if isinstance(source_df, pd.DataFrame) and not source_df.empty:
            st.dataframe(source_df, use_container_width=True, hide_index=True)
        else:
            st.caption("Пока нет данных по лучшим поставщикам.")

    with st.expander("8. Задачи / напоминания ❔", expanded=False):
        st.caption(
            "Что показывает: задачи по карточкам на пересмотр, проверку или доработку. "
            "Как пользоваться: открывай карточку из задачи, проверяй позицию и отмечай задачу выполненной."
        )
        task_df = build_task_view_df(sheet_name)
        render_tasks_table_ui(task_df, f"analytics_tasks_{tab_key}", default_sheet=sheet_name)

    st.download_button(
        "⬇️ Скачать аналитику в Excel",
        analytics_bundle_to_excel_bytes(bundle),
        file_name=f"analytics_{tab_key}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"download_analytics_{tab_key}",
    )


def build_crm_header_stats(
    sheet_df: pd.DataFrame | None,
    photo_df: pd.DataFrame | None,
    avito_df: pd.DataFrame | None,
    sheet_name: str,
    tab_label: str,
    min_qty: float,
) -> dict[str, int]:
    stats = {
        "tasks_open": 0,
        "tasks_overdue": 0,
        "can_buy": 0,
        "without_photo": 0,
        "without_avito": 0,
    }

    task_df = load_task_registry_df()
    if isinstance(task_df, pd.DataFrame) and not task_df.empty:
        sheet_mask = (
            task_df.get("sheet_name", pd.Series(dtype=object))
            .fillna("")
            .map(normalize_text)
            .eq(normalize_text(sheet_name))
        )
        task_sheet = task_df.loc[sheet_mask].copy()
        if not task_sheet.empty:
            effective_status = task_sheet.apply(lambda r: task_effective_status(r), axis=1)
            stats["tasks_open"] = int(effective_status.isin(["NEW", "ACTIVE", "OVERDUE"]).sum())
            stats["tasks_overdue"] = int(effective_status.eq("OVERDUE").sum())

    hot_buy_df = build_hot_buy_watchlist_table()
    if isinstance(hot_buy_df, pd.DataFrame) and not hot_buy_df.empty and "Лист" in hot_buy_df.columns:
        stats["can_buy"] = int((hot_buy_df["Лист"].fillna("").astype(str) == tab_label).sum())

    if isinstance(sheet_df, pd.DataFrame) and not sheet_df.empty:
        registry_df = load_avito_registry_df()
        bundle = build_operational_analytics_bundle(sheet_df, photo_df, avito_df, registry_df, float(min_qty), str(tab_label))
        quality = bundle.get("quality", {}) if isinstance(bundle, dict) else {}
        stats["without_photo"] = int(quality.get("without_photo", 0) or 0)
        stats["without_avito"] = int(quality.get("in_price_not_in_avito", 0) or 0)
    return stats


def render_crm_header_bar(
    sheet_df: pd.DataFrame | None,
    photo_df: pd.DataFrame | None,
    avito_df: pd.DataFrame | None,
    sheet_name: str,
    tab_label: str,
    min_qty: float,
) -> None:
    stats = build_crm_header_stats(sheet_df, photo_df, avito_df, sheet_name, tab_label, min_qty)
    st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
    render_block_header(
        "CRM — быстрый центр управления",
        "Короткая шапка с главными рабочими зонами: задачи, выгодные закупки, проблемы по карточкам.",
        icon="🧭",
        help_text="Эта шапка ничего не меняет в comparison. Она только помогает быстро открыть нужный рабочий блок без долгой прокрутки.",
    )
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        open_tasks = bool(st.checkbox(
            f"🔔 Задачи ({stats['tasks_open']})",
            value=bool(st.session_state.get(f"crm_show_tasks_{sheet_name}", st.session_state.get("show_task_center_global", False))),
            key=f"crm_show_tasks_{sheet_name}",
            help="Открывает единый центр задач: новые, активные, просроченные и выполненные.",
        ))
        st.caption(f"Просрочено: {stats['tasks_overdue']}")
    with c2:
        open_buy = bool(st.checkbox(
            f"💸 Можно брать ({stats['can_buy']})",
            value=bool(st.session_state.get(f"crm_show_buy_{sheet_name}", st.session_state.get("show_hot_buy_watchlist_table", False))),
            key=f"crm_show_buy_{sheet_name}",
            help="Открывает ленивую таблицу по ходовым позициям, где поставщик сейчас даёт выгодный вход по цене.",
        ))
        st.caption("Показывает только выгодные позиции")
    with c3:
        st.metric("🖼️ Нет фото", stats["without_photo"])
        st.caption("Сколько позиций на листе без фото")
    with c4:
        st.metric("🛒 Без Avito", stats["without_avito"])
        st.caption("Сколько позиций на листе без объявления")
    with c5:
        st.metric("Лист", tab_label)
        st.caption("CRM-метрики именно по текущему листу")
    st.markdown('</div>', unsafe_allow_html=True)


def render_crm_card_center(
    result_df: pd.DataFrame | None,
    display_result_df: pd.DataFrame | None,
    compare_map: dict[str, dict[str, Any]] | None,
    avito_df: pd.DataFrame | None,
    sheet_name: str,
    tab_label: str,
    tab_key: str,
    price_mode: str,
    round100: bool,
    custom_discount: float,
) -> None:
    if not isinstance(display_result_df, pd.DataFrame) or display_result_df.empty:
        return

    rows = display_result_df.copy()
    options = []
    option_map: dict[str, pd.Series] = {}
    for _, row in rows.iterrows():
        art = normalize_text(row.get("article", ""))
        name = normalize_text(row.get("name", ""))
        label = f"{art} — {name[:120]}"
        options.append(label)
        option_map[label] = row

    st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
    render_block_header(
        "CRM-карточка товара",
        "Один компактный экран по позиции: обзор, цены, Avito, заметки и задачи. Старые блоки остаются ниже как вторичный режим.",
        icon="🧩",
        help_text="Нужна, чтобы не бегать по странице: всё важное по найденной позиции собрано в одном месте.",
    )
    selected_label = st.selectbox(
        "Позиция для CRM-карточки",
        options,
        index=0 if options else None,
        key=f"crm_card_select_{tab_key}",
        help="Выбери найденную позицию, чтобы открыть её краткую CRM-карточку.",
    )
    if not selected_label:
        st.markdown('</div>', unsafe_allow_html=True)
        return

    row = option_map[selected_label]
    art = normalize_text(row.get("article", ""))
    art_norm = normalize_text(row.get("article_norm", ""))
    name = normalize_text(row.get("name", ""))
    photo_url = normalize_text(row.get("photo_url", ""))
    note = normalize_text(row.get("manual_note", ""))
    model = normalize_text(row.get("meta_model", ""))
    mcode = normalize_text(row.get("meta_manufacturer_code", ""))
    fits = normalize_text(row.get("meta_fits_models", ""))
    own_price = get_selected_price_raw(row, price_mode, round100, custom_discount)
    own_stock = parse_qty_generic(row.get("free_qty"))
    best = get_best_offer(row, min_qty=float(st.session_state.get("distributor_min_qty", 1.0)))
    best_source = normalize_text((best or {}).get("source", ""))
    best_price = safe_float((best or {}).get("price"), 0.0)
    best_qty = safe_float((best or {}).get("qty"), 0.0)
    matched_ads = pd.DataFrame()
    if isinstance(avito_df, pd.DataFrame) and not avito_df.empty:
        one_row = pd.DataFrame([row.to_dict()])
        matched_ads = find_avito_ads(avito_df, one_row)

    t_overview, t_prices, t_avito, t_notes = st.tabs(["Обзор", "Цены", "Avito", "Заметки / задачи"])

    with t_overview:
        c1, c2 = st.columns([1, 2])
        with c1:
            if photo_url:
                st.image(photo_url, use_container_width=True)
            else:
                st.info("Фото не найдено")
        with c2:
            st.markdown(f"### {html.escape(art)}")
            st.caption(name)
            m1, m2, m3 = st.columns(3)
            m1.metric("Наша цена", fmt_price(own_price))
            m2.metric("Наш склад", fmt_qty(own_stock))
            m3.metric("Лучший поставщик", best_source or "—")
            if note:
                st.info(f"Заметка: {note}")
            if model or mcode or fits:
                if model:
                    st.write(f"**Модель:** {model}")
                if mcode:
                    st.write(f"**Код производителя:** {mcode}")
                if fits:
                    st.write(f"**Подходит к моделям:** {fits}")

    with t_prices:
        st.caption("Здесь видно нашу цену и лучший рынок по текущей позиции. Это быстрый обзор, а полный блок 'Показать цены у всех' остаётся ниже.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Наша цена", fmt_price(own_price))
        c2.metric("Лучший поставщик", best_source or "—")
        c3.metric("Цена поставщика", fmt_price(best_price) if best_price > 0 else "—")
        c4.metric("Остаток поставщика", fmt_qty(best_qty) if best_qty > 0 else "—")
        if isinstance(compare_map, dict):
            data = compare_map.get(art_norm) or compare_map.get(art) or {}
            if data:
                rows_prices = []
                for item in data.get("offers", [])[:20]:
                    rows_prices.append({
                        "Поставщик": normalize_text(item.get("source", "")),
                        "Цена": fmt_price(safe_float(item.get("price"), 0.0)),
                        "Остаток": fmt_qty(safe_float(item.get("qty"), 0.0)),
                    })
                if rows_prices:
                    st.dataframe(pd.DataFrame(rows_prices), use_container_width=True, hide_index=True)

    with t_avito:
        st.caption("Связанные объявления Avito по этой позиции. Отсюда можно быстро понять, на каком аккаунте она висит и есть ли вообще объявление.")
        if isinstance(matched_ads, pd.DataFrame) and not matched_ads.empty:
            try:
                render_avito_block(avito_df, pd.DataFrame([row.to_dict()]))
            except Exception:
                view_cols = [c for c in ["ad_id", "title", "account", "price", "last_changed_at"] if c in matched_ads.columns]
                st.dataframe(matched_ads[view_cols], use_container_width=True, hide_index=True)
        else:
            st.info("Связанные объявления Avito не найдены.")

    with t_notes:
        st.caption("Здесь можно быстро поправить карточку и сразу создать задачу на пересмотр без переходов вниз по странице.")
        current_photo = photo_url
        current_name = name
        current_model = model
        current_code = mcode
        current_fits = fits
        current_note = note

        with st.form(f"crm_card_form_{tab_key}_{art_norm}", clear_on_submit=False):
            cc1, cc2 = st.columns([1.2, 1.8])
            with cc1:
                photo_url_new = st.text_input("Фото (ссылка)", value=current_photo, key=f"crm_card_photo_{tab_key}_{art_norm}")
            with cc2:
                name_new = st.text_area("Название", value=current_name, height=90, key=f"crm_card_name_{tab_key}_{art_norm}")
            mm1, mm2 = st.columns(2)
            model_new = mm1.text_input("Модель", value=current_model, key=f"crm_card_model_{tab_key}_{art_norm}")
            code_new = mm2.text_input("Код производителя", value=current_code, key=f"crm_card_code_{tab_key}_{art_norm}")
            fits_new = st.text_area("Подходит к моделям", value=current_fits, height=80, key=f"crm_card_fits_{tab_key}_{art_norm}")
            note_new = st.text_area("Заметка", value=current_note, height=70, key=f"crm_card_note_{tab_key}_{art_norm}")

            st.markdown("#### 🔔 Создать задачу по карточке")
            st.caption("Заметка — это просто комментарий. Задача — отдельное напоминание со сроком, статусом и причиной.")
            tt1, tt2, tt3 = st.columns([1.1, 1.4, 1.7])
            make_task = tt1.checkbox(
                "Создать задачу",
                key=f"crm_card_make_task_{tab_key}_{art_norm}",
                help="Создаёт напоминание по этой позиции. Оно попадёт в общий центр задач и не пропадёт после загрузки нового файла.",
            )
            due_date = tt2.date_input(
                "Когда проверить",
                value=(datetime.utcnow().date() + timedelta(days=14)),
                key=f"crm_card_due_{tab_key}_{art_norm}",
            )
            task_reason = tt3.selectbox(
                "Причина",
                ["Пересмотреть цену", "Проверить после правки", "Нет продаж", "Проверить фото/карточку", "Проверить спрос", "Другое"],
                key=f"crm_card_reason_{tab_key}_{art_norm}",
            )
            task_note = st.text_area(
                "Комментарий к задаче",
                value="",
                height=65,
                key=f"crm_card_task_note_{tab_key}_{art_norm}",
                placeholder="Например: снизили цену, проверить продажи через 14 дней.",
            )

            sb1, sb2 = st.columns(2)
            save_clicked = sb1.form_submit_button("💾 Сохранить карточку", use_container_width=True, type="primary")
            reset_clicked = sb2.form_submit_button("↺ Сбросить ручные правки", use_container_width=True)

        if save_clicked:
            save_card_override(
                sheet_name,
                art,
                art_norm,
                {
                    "photo_url": photo_url_new,
                    "name_override": name_new,
                    "meta_model": model_new,
                    "meta_manufacturer_code": code_new,
                    "meta_fits_models": fits_new,
                    "note": note_new,
                },
            )
            if make_task:
                create_review_task(
                    article=art,
                    article_norm=art_norm,
                    sheet_name=sheet_name,
                    name_snapshot=name_new or current_name,
                    due_date=due_date,
                    reason=task_reason,
                    note=task_note or note_new,
                    source="crm_card",
                )
            st.success(f"Карточка {art} сохранена.")
            if make_task:
                st.info(f"Задача по {art} создана до {due_date}.")
            st.rerun()

        if reset_clicked:
            delete_card_override(sheet_name, art_norm)
            st.success(f"Ручные правки для {art} сброшены.")
            st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

def render_sheet_workspace(sheet_name: str, tab_label: str, tab_key: str) -> None:
    search_key = f"search_input_{tab_key}"
    search_widget_key = f"search_input_widget_{tab_key}"
    search_widget_pending_key = f"search_input_widget_pending_{tab_key}"
    clear_flag_key = f"search_clear_requested_{tab_key}"
    submitted_key = f"submitted_query_{tab_key}"
    result_key = f"last_result_{tab_key}"
    sig_key = f"last_result_sig_{tab_key}"
    if search_key not in st.session_state:
        st.session_state[search_key] = ""
    if submitted_key not in st.session_state:
        st.session_state[submitted_key] = ""
    if result_key not in st.session_state:
        st.session_state[result_key] = None
    if sig_key not in st.session_state:
        st.session_state[sig_key] = None
    if search_widget_key not in st.session_state:
        st.session_state[search_widget_key] = st.session_state[search_key]
    if clear_flag_key not in st.session_state:
        st.session_state[clear_flag_key] = False
    pending_search_value = st.session_state.pop(search_widget_pending_key, None)
    if pending_search_value is not None:
        st.session_state[search_widget_key] = pending_search_value
    if st.session_state.get(clear_flag_key):
        st.session_state[search_key] = ""
        st.session_state[search_widget_key] = ""
        st.session_state[clear_flag_key] = False

    base_sheet_raw = sheets.get(sheet_name) if isinstance(sheets, dict) else None
    base_sheet_df = apply_card_overrides(base_sheet_raw.copy(), sheet_name) if isinstance(base_sheet_raw, pd.DataFrame) else None
    show_photos = bool(st.session_state.get("show_photos_global", True))
    photo_df = st.session_state.get("photo_df")
    source_pairs = get_source_pairs(base_sheet_df) if isinstance(base_sheet_df, pd.DataFrame) else []

    st.markdown('<div class="toolbar">', unsafe_allow_html=True)
    render_block_header(
        f"{tab_label} — поиск товара",
        f"Работа только по листу «{sheet_name}». Рендерится только текущий раздел — это ускоряет приложение.",
        icon="🔎",
        help_text="Поиск работает только по активному листу comparison-файла. Мы не рендерим все разделы сразу и не делаем лишний rerun после каждого клика.",
    )

    with st.form(f"search_form_{tab_key}", clear_on_submit=False):
        search_value = st.text_area(
            "Поисковый запрос",
            key=search_widget_key,
            placeholder="Например:\nCE278A CE285A\nили\n001R00600 / 006R01464",
            height=90,
            label_visibility="collapsed",
        )
        c1, c2, c3 = st.columns([1, 1, 2.4])
        find_clicked = c1.form_submit_button("🔎 Найти", use_container_width=True, type="primary")
        clear_clicked = c2.form_submit_button("🧹 Очистить", use_container_width=True)
        c3.markdown(
            f"<div style='padding-top:9px;color:#64748b;font-size:12px;'>Тип поиска сейчас: <b>{html.escape(search_mode)}</b>. Для коротких OEM-кодов вроде TK-8600Y используй режим «Умный».</div>",
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)

    result_df = st.session_state.get(result_key)

    if clear_clicked:
        st.session_state[search_key] = ""
        st.session_state[submitted_key] = ""
        st.session_state[result_key] = None
        st.session_state[sig_key] = None
        st.session_state[search_widget_pending_key] = ""
        st.session_state[clear_flag_key] = True
        result_df = None
        st.rerun()
    elif find_clicked:
        normalized_query = normalize_query_for_display(search_value)
        st.session_state[search_key] = normalized_query
        st.session_state[submitted_key] = normalized_query
        desired_sig = (normalized_query, search_mode, sheet_name, st.session_state.get("comparison_version", ""))
        if isinstance(base_sheet_df, pd.DataFrame) and normalize_text(normalized_query):
            if st.session_state.get(sig_key) != desired_sig or result_df is None:
                result_df = search_in_df(base_sheet_df, normalized_query, search_mode, sheet_name=sheet_name)
                st.session_state[result_key] = result_df
                st.session_state[sig_key] = desired_sig
        else:
            result_df = None
            st.session_state[result_key] = None
            st.session_state[sig_key] = desired_sig

    submitted_query = st.session_state.get(submitted_key, "")
    desired_sig = (submitted_query, search_mode, sheet_name, st.session_state.get("comparison_version", ""))
    if isinstance(base_sheet_df, pd.DataFrame) and normalize_text(submitted_query):
        if st.session_state.get(sig_key) != desired_sig or result_df is None:
            result_df = search_in_df(base_sheet_df, submitted_query, search_mode, sheet_name=sheet_name)
            st.session_state[result_key] = result_df
            st.session_state[sig_key] = desired_sig
    else:
        result_df = None

    min_dist_qty = float(st.session_state.get("distributor_min_qty", 1.0))
    series_df = base_sheet_df.copy() if isinstance(base_sheet_df, pd.DataFrame) else None

    if isinstance(base_sheet_df, pd.DataFrame) and normalize_text(submitted_query):
        series_info = get_series_candidates(series_df, submitted_query)
    else:
        series_info = {"prefix": "", "candidates": []}
    series_candidates = series_info.get("candidates", []) if isinstance(series_info, dict) else []
    if isinstance(base_sheet_df, pd.DataFrame) and normalize_text(submitted_query) and series_candidates:
        st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
        render_block_header(
            f"{tab_label} — серия / группа по части артикула",
            "Если вводишь только часть артикула, здесь можно быстро выбрать всю группу и одним кликом добавить нужные позиции в поиск.",
            icon="🎨",
            help_text="Подходит для цветов, ёмкостей и серийных товаров: CE505, TK-8600, CTL-1100 и похожих групп.",
        )
        st.caption(f"По префиксу {series_info.get('prefix', '')} найдено позиций: {len(series_candidates)}")
        c_add, c_all, c_clear = st.columns(3)
        prefix_key = f"{tab_key}_{normalize_article(str(series_info.get('prefix', '')))}"
        select_all_clicked = c_all.button("Выбрать все", use_container_width=True, key=f"series_select_all_{prefix_key}")
        clear_all_clicked = c_clear.button("Очистить выбор", use_container_width=True, key=f"series_clear_all_{prefix_key}")
        if select_all_clicked:
            st.session_state[f"series_selected_{prefix_key}"] = [str(c["article_norm"]) for c in series_candidates]
        if clear_all_clicked:
            st.session_state[f"series_selected_{prefix_key}"] = []
        options = [str(c["article_norm"]) for c in sorted(series_candidates, key=series_sort_key)]
        format_map = {}
        for c in series_candidates:
            norm = str(c["article_norm"])
            label = f"🟢 {c['article']} — свободно: {fmt_qty(c['free_qty'])} • {fmt_price_with_rub(c['sale_price'])} • {c['name']}"
            format_map[norm] = label
        selected_norms = st.multiselect(
            "Выберите позиции серии",
            options=options,
            default=st.session_state.get(f"series_selected_{prefix_key}", []),
            format_func=lambda x: format_map.get(x, x),
            key=f"series_multiselect_{prefix_key}",
            label_visibility="collapsed",
        )
        st.session_state[f"series_selected_{prefix_key}"] = selected_norms
        add_clicked = c_add.button("Добавить отмеченные в поиск", use_container_width=True, key=f"series_add_{prefix_key}")
        if add_clicked and selected_norms:
            selected_articles = []
            selected_set = set(selected_norms)
            for c in series_candidates:
                norm = str(c["article_norm"])
                if norm not in selected_set:
                    continue
                selected_articles.append(str(c["article"]))
            if selected_articles:
                normalized_query = "\n".join(unique_preserve_order(selected_articles))
                st.session_state[search_key] = normalized_query
                st.session_state[search_widget_pending_key] = normalized_query
                st.session_state[submitted_key] = normalized_query
                result_df = search_in_df(base_sheet_df, normalized_query, search_mode, sheet_name=sheet_name)
                st.session_state[result_key] = result_df
                st.session_state[sig_key] = (normalized_query, search_mode, sheet_name, st.session_state.get("comparison_version", ""))
                submitted_query = normalized_query
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    if not isinstance(base_sheet_df, pd.DataFrame):
        render_info_banner(
            f"Вкладка «{tab_label}» пока пуста",
            f"В comparison-файле не найден лист «{sheet_name}».",
            icon="📭",
            chips=["проверь названия листов", "ожидаются: Сравнение / Уценка / Совместимые"],
            tone="purple",
        )
        return

    display_result_df = result_df
    hot_items_df = st.session_state.get("hot_items_df")
    if isinstance(result_df, pd.DataFrame) and sheet_name == "Совместимые" and not result_df.empty:
        compatible_result = result_df.copy()
        compatible_result["compatible_brand"] = compatible_result.apply(
            lambda row: extract_compatible_brand(row.get("name", ""), row.get("article", "")),
            axis=1,
        )
        brand_options = [b for b in sorted({normalize_text(x) for x in compatible_result["compatible_brand"].tolist() if normalize_text(x)})]
        selected_brand = st.selectbox(
            "Фильтр по бренду совместимки",
            ["Все бренды", *brand_options],
            key="compatible_brand_filter",
            help="Бренд берётся из названия совместимой позиции; если не распознан, строка остаётся только в режиме 'Все бренды'.",
        )
        if selected_brand != "Все бренды":
            compatible_result = compatible_result[compatible_result["compatible_brand"] == selected_brand].reset_index(drop=True)
        result_df = compatible_result
        st.session_state[result_key] = result_df
    if isinstance(result_df, pd.DataFrame) and show_photos:
        display_result_df = apply_photo_map(result_df, photo_df)
    if isinstance(display_result_df, pd.DataFrame):
        display_result_df = apply_card_overrides(display_result_df, sheet_name)
    if isinstance(display_result_df, pd.DataFrame) and isinstance(hot_items_df, pd.DataFrame) and not hot_items_df.empty:
        display_result_df = apply_hot_watchlist(display_result_df, hot_items_df, tab_label=tab_label)

    if result_df is None:
        render_info_banner(
            f"{tab_label}: лист загружен",
            f"Теперь введите артикул или несколько артикулов для поиска по листу «{sheet_name}».",
            icon="✅",
            chips=[f"строк: {len(base_sheet_df)}", "активен только один раздел", "тяжёлые блоки по запросу"],
            tone="green",
        )
    else:
        st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
        render_block_header(
            f"{tab_label} — результаты поиска",
            "Главная таблица по найденным позициям. Тяжёлые блоки ниже можно включать только когда они реально нужны.",
            icon="📋",
            help_text="Поиск работает только по текущему разделу. Фото можно отключать глобально, а тяжёлые блоки вроде 'цены у всех', Авито и полного отчёта считаются только по запросу.",
        )
        if display_result_df.empty:
            st.warning("Ничего не найдено. Попробуйте другой артикул или часть названия.")
        else:
            compare_map = build_distributor_compare(result_df, min_qty=min_dist_qty)
            render_results_insight_dashboard(display_result_df, compare_map, source_pairs)
            render_results_table(display_result_df.head(200), price_mode, round100, custom_discount, distributor_map=compare_map, show_photos=show_photos)
            st.download_button(
                "⬇️ Скачать результаты в Excel",
                to_excel_bytes(display_result_df, price_mode, round100, custom_discount, min_dist_qty),
                file_name=f"moy_tovar_search_results_{tab_key}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"download_results_{tab_key}",
            )
            with st.expander("Показать техническую таблицу"):
                tech = display_result_df.copy()
                tech["Наша цена"] = tech["sale_price"].map(fmt_price)
                tech["Наш склад"] = tech["free_qty"].map(fmt_qty)
                tech["Лучшая цена"] = tech.apply(lambda row: (get_best_offer(row, min_qty=min_dist_qty) or {}).get("price_fmt", ""), axis=1)
                tech["Лучший поставщик"] = tech.apply(lambda row: (get_best_offer(row, min_qty=min_dist_qty) or {}).get("source", ""), axis=1)
                tech["Фото"] = tech.get("photo_url", "")
                if "hot_flag" in tech.columns:
                    tech["Ходовая"] = tech["hot_flag"].map(lambda x: "Да" if bool(x) else "")
                    tech["Action"] = tech.get("hot_action_today", "")
                    tech = tech[["article", "name", "Наша цена", "Наш склад", "Лучший поставщик", "Лучшая цена", "Ходовая", "Action", "Фото"]].rename(columns={"article": "Артикул", "name": "Название"})
                else:
                    tech = tech[["article", "name", "Наша цена", "Наш склад", "Лучший поставщик", "Лучшая цена", "Фото"]].rename(columns={"article": "Артикул", "name": "Название"})
                st.dataframe(tech, use_container_width=True, hide_index=True)

            render_crm_card_center(
                result_df,
                display_result_df,
                compare_map,
                st.session_state.get("avito_df"),
                sheet_name,
                tab_label,
                tab_key,
                price_mode,
                round100,
                custom_discount,
            )

            render_card_editor_panel(display_result_df, sheet_name, tab_key)

            lazy_c0, lazy_c1, lazy_c2, lazy_c3, lazy_c4, lazy_c5 = st.columns(6)
            lazy_c0.checkbox("Показать шаблоны", key=f"lazy_templates_{tab_key}")
            lazy_c1.checkbox("Показать цены у всех", key=f"lazy_all_prices_{tab_key}")
            lazy_c2.checkbox("Файл для руководителя", key=f"lazy_analysis_{tab_key}")
            lazy_c3.checkbox("Показать Авито", key=f"lazy_avito_{tab_key}")
            lazy_c4.checkbox("Считать отчёт по листу", key=f"lazy_report_{tab_key}")
            lazy_c5.checkbox("Аналитика / задачи", key=f"lazy_analytics_{tab_key}")

            if st.session_state.get(f"lazy_templates_{tab_key}", False):
                result_enriched_for_templates = apply_photo_map(result_df, photo_df) if isinstance(result_df, pd.DataFrame) else result_df
                if isinstance(result_enriched_for_templates, pd.DataFrame):
                    result_enriched_for_templates = apply_card_overrides(result_enriched_for_templates, sheet_name)
                st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
                render_block_header(
                    f"{tab_label} — шаблоны",
                    "Два быстрых шаблона для ответа или публикации по найденным позициям.",
                    icon="🧾",
                )
                t1, t2 = st.columns(2)
                with t1:
                    template1 = build_offer_template_from_result_df(result_enriched_for_templates, round100, st.session_state.template1_footer)
                    st.session_state[f"template1_{tab_key}"] = template1
                    st.text_area("Шаблон 1", height=300, key=f"template1_{tab_key}")
                with t2:
                    template2 = build_selected_price_template_from_result_df(result_enriched_for_templates, price_mode, round100, custom_discount)
                    st.session_state[f"template2_{tab_key}"] = template2
                    st.text_area("Шаблон 2", height=300, key=f"template2_{tab_key}")
                st.markdown('</div>', unsafe_allow_html=True)

            if st.session_state.get(f"lazy_all_prices_{tab_key}", False):
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
                render_all_prices_block(result_df, min_dist_qty, price_mode, round100, custom_discount, widget_key_prefix=tab_key)
                st.markdown('</div>', unsafe_allow_html=True)

            if st.session_state.get(f"lazy_analysis_{tab_key}", False):
                st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
                render_info_banner(
                    "Файл для согласования с руководителем",
                    "Этот экспорт собирает базовую аналитику по найденным товарам: ваш текущий прод, лучшую цену поставщика и поля, которые удобно дозаполнить вручную перед обсуждением новых цен.",
                    icon="🗂️",
                    chips=["артикул и количество уже заполнены", "лучшая цена дистрибьютора уже внутри", "готово для обсуждения"],
                    tone="blue",
                )
                st.download_button(
                    "⬇️ Скачать анализ товара",
                    build_product_analysis_workbook_bytes(result_df, min_qty=min_dist_qty),
                    file_name=f"analysis_for_manager_{tab_key}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=False,
                    key=f"download_analysis_{tab_key}",
                )
                st.markdown('</div>', unsafe_allow_html=True)

            if st.session_state.get(f"lazy_avito_{tab_key}", False) and isinstance(st.session_state.get("avito_df"), pd.DataFrame) and not st.session_state.avito_df.empty:
                st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
                render_block_header(
                    f"{tab_label} — Авито",
                    "Проверка, есть ли по найденным артикулам объявления в загруженном файле Авито.",
                    icon="🛒",
                )
                render_avito_block(st.session_state.avito_df, result_df)
                st.markdown('</div>', unsafe_allow_html=True)

            if st.session_state.get(f"lazy_analytics_{tab_key}", False):
                st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
                render_block_header(
                    f"{tab_label} — аналитика / задачи",
                    "Операционная аналитика по текущему листу: приоритет на пересмотр цены, проблемные позиции, качество карточек, серии, история правок и действия на сегодня.",
                    icon="📌",
                    help_text="Блок считается лениво и открывается только по чекбоксу. Аналитика строится по текущему листу и не должна влиять на обычный поиск, пока выключена.",
                )
                render_operational_analytics_block(
                    base_sheet_df,
                    photo_df,
                    st.session_state.get("avito_df"),
                    st.session_state.distributor_min_qty,
                    tab_label,
                    tab_key,
                )
                st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.get(f"lazy_report_{tab_key}", False):
        st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
        render_block_header(
            f"{tab_label} — отчёт по листу",
            "Полный отчёт по выбранному листу: где поставщик реально дешевле нас на заданный процент. Если загружен watchlist, отчёт дополняется спросом, днями запаса, приоритетом и действием.",
            icon="📊",
            help_text="Отчёт строится по всему текущему листу, а не только по поисковой выдаче. Порог и минимальный остаток меняются в sidebar.",
        )
        report_hot_lookup = build_hot_watchlist_lookup(st.session_state.get("hot_items_df"), tab_label)
        report_df = build_report_df(
            base_sheet_df,
            st.session_state.distributor_threshold,
            st.session_state.distributor_min_qty,
            tab_label=tab_label,
            hot_lookup=report_hot_lookup,
        )
        if report_df.empty:
            st.info("По текущему листу нет позиций, которые проходят ваш порог выгоды.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Строк в отчёте", len(report_df))
            c2.metric("Порог", f"{fmt_qty(st.session_state.distributor_threshold)}%")
            c3.metric("Источников", len(source_pairs))

            f1, f2, f3 = st.columns(3)
            only_hot = f1.checkbox(
                "Только ходовые",
                key=f"report_only_hot_{tab_key}",
                help="Показывать только товары, которые хорошо продаются за выбранный период.",
            )
            only_buy = f2.checkbox(
                "Только можно брать",
                key=f"report_only_buy_{tab_key}",
                help="Показывать только позиции, где лучший поставщик сейчас минимум на 35% дешевле нашей цены.",
            )
            only_attention = f3.checkbox(
                "Только требует внимания",
                key=f"report_only_attention_{tab_key}",
                help="Показывать только позиции, где нужно действие: пополнить запас, наблюдать или позиция не найдена в сравнении.",
            )

            st.caption(
                "Только ходовые — товары с хорошим спросом. "
                "Только можно брать — позиции, где поставщик сейчас минимум на 35% дешевле нашей цены. "
                "Только требует внимания — позиции, где нужно действие: пополнить запас, наблюдать или проверить сравнение."
            )

            filtered_report_df = report_df.copy()

            if only_hot:
                filtered_report_df = filtered_report_df[
                    filtered_report_df["Ходовая"].astype(str).str.strip().eq("Да")
                ]

            if only_buy:
                filtered_report_df = filtered_report_df[
                    filtered_report_df["Действие"]
                    .fillna("")
                    .astype(str)
                    .str.upper()
                    .str.contains("МОЖНО БРАТЬ", regex=False)
                ]

            if only_attention:
                filtered_report_df = filtered_report_df[
                    filtered_report_df["Действие"]
                    .fillna("")
                    .astype(str)
                    .str.upper()
                    .str.contains("ПОПОЛНИТЬ ЗАПАС|НАБЛЮДАТЬ|НЕТ В СРАВНЕНИИ", regex=True)
                ]

            if filtered_report_df.empty:
                st.info("По выбранным фильтрам строк не найдено.")
            else:
                st.dataframe(filtered_report_df, use_container_width=True, hide_index=True, height=420)
                st.download_button(
                    "⬇️ Скачать отчёт по листу",
                    report_to_excel_bytes(filtered_report_df),
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
    if "show_photos_global" not in st.session_state:
        st.session_state["show_photos_global"] = True

    tab_specs = [
        ("Сравнение", "Оригинал", "original"),
        ("Уценка", "Уценка", "discount"),
        ("Совместимые", "Совместимые", "compatible"),
    ]
    label_to_spec = {label: (sheet_name, label, tab_key) for sheet_name, label, tab_key in tab_specs}
    if "active_workspace_label" not in st.session_state:
        st.session_state["active_workspace_label"] = "Оригинал"

    task_counts = task_summary_counts()
    switch_l, switch_m, switch_r = st.columns([3.2, 1.25, 1.25])
    switch_l.radio(
        "Раздел",
        options=[label for _, label, _ in tab_specs],
        key="active_workspace_label",
        horizontal=True,
        label_visibility="collapsed",
    )
    switch_m.checkbox(
        f"🔔 Задачи ({task_counts.get('open', 0)})",
        key="show_task_center_global",
        help="Открывает ленивый список задач и напоминаний по карточкам. Пока чекбокс выключен, список не строится.",
    )
    switch_r.checkbox("Показать фото", key="show_photos_global")

    active_sheet_name, active_tab_label, active_tab_key = label_to_spec[st.session_state.get("active_workspace_label", "Оригинал")]
    st.session_state["active_workspace_sheet_name"] = active_sheet_name
    active_sheet_df = sheets.get(active_sheet_name) if isinstance(sheets, dict) else None
    render_crm_header_bar(
        active_sheet_df,
        st.session_state.get("photo_df"),
        st.session_state.get("avito_df"),
        active_sheet_name,
        active_tab_label,
        st.session_state.get("distributor_min_qty", 1.0),
    )
    render_task_center_lazy_panel()
    render_hot_buy_watchlist_lazy_panel()
    render_sheet_workspace(active_sheet_name, active_tab_label, active_tab_key)

def normalize_watchlist_sheet_name(value: Any) -> str:
    txt = contains_text(value)
    if "ОРИГИН" in txt or "СРАВН" in txt:
        return "Оригинал"
    if "УЦЕН" in txt:
        return "Уценка"
    if "СОВМЕСТ" in txt:
        return "Совместимые"
    return normalize_text(value)


@st.cache_data(show_spinner=False, ttl=3600, max_entries=4)
def load_hot_watchlist_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix == ".csv":
        bio = io.BytesIO(file_bytes)
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
    else:
        raw = pd.read_excel(io.BytesIO(file_bytes))
    raw = raw.dropna(how="all").copy()
    if raw.empty:
        return pd.DataFrame(columns=[
            "watch_article", "watch_key", "watch_name", "current_sheet", "comparison_article",
            "sales_qty_15m", "sales_per_month", "abc_class", "velocity_band",
            "best_supplier", "best_supplier_gap_pct", "buy_signal_30pct", "days_of_cover",
            "priority_score", "action_today", "watch_article_norm", "watch_key_norm",
            "comparison_article_norm", "match_keys_text",
        ])
    raw.columns = [normalize_text(c) for c in raw.columns]
    rows = []
    for _, r in raw.iterrows():
        watch_article = normalize_text(r.get("watch_article", ""))
        watch_key = normalize_text(r.get("watch_key", ""))
        watch_name = normalize_text(r.get("watch_name", ""))
        comparison_article = normalize_text(r.get("comparison_article", ""))
        keys = unique_preserve_order([
            normalize_article(watch_article),
            normalize_article(watch_key),
            normalize_article(comparison_article),
        ])
        if not any(keys):
            continue
        rows.append({
            "watch_article": watch_article,
            "watch_key": watch_key,
            "watch_name": watch_name,
            "current_sheet": normalize_watchlist_sheet_name(r.get("current_sheet", "")),
            "comparison_article": comparison_article,
            "sales_qty_15m": safe_float(r.get("sales_qty_15m"), 0.0),
            "sales_per_month": safe_float(r.get("sales_per_month"), 0.0),
            "abc_class": normalize_text(r.get("abc_class", "")),
            "velocity_band": normalize_text(r.get("velocity_band", "")),
            "best_supplier": normalize_text(r.get("best_supplier", "")),
            "best_supplier_gap_pct": safe_float(r.get("best_supplier_gap_pct"), 0.0),
            "buy_signal_30pct": normalize_text(r.get("buy_signal_30pct", "")),
            "days_of_cover": safe_float(r.get("days_of_cover"), 0.0),
            "priority_score": safe_float(r.get("priority_score"), 0.0),
            "action_today": normalize_text(r.get("action_today", "")),
            "watch_article_norm": normalize_article(watch_article),
            "watch_key_norm": normalize_article(watch_key),
            "comparison_article_norm": normalize_article(comparison_article),
            "match_keys_text": "|".join([k for k in keys if k]),
        })
    out = pd.DataFrame(rows)
    return out.reset_index(drop=True)


def build_hot_watchlist_lookup(hot_df: pd.DataFrame | None, tab_label: str = "") -> dict[str, list[dict[str, Any]]]:
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return {}
    work = hot_df.copy()
    tab_label = normalize_text(tab_label)
    if tab_label:
        filtered = work[(work["current_sheet"] == "") | (work["current_sheet"] == tab_label)].copy()
        if not filtered.empty:
            work = filtered
    lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for _, row in work.iterrows():
        rec = row.to_dict()
        keys = [normalize_article(x) for x in normalize_text(rec.get("match_keys_text", "")).split("|") if normalize_article(x)]
        if not keys:
            continue
        for key in keys:
            lookup[key].append(rec)
    return lookup


def pick_hot_watch_rec(row: pd.Series, lookup: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    if not lookup:
        return None
    candidate_keys = []
    article_norm = normalize_article(row.get("article_norm", row.get("article", "")))
    if article_norm:
        candidate_keys.append(article_norm)
    row_codes = row.get("row_codes")
    if isinstance(row_codes, list):
        candidate_keys.extend([normalize_article(x) for x in row_codes if normalize_article(x)])
    best = None
    best_score = -10**18
    seen = set()
    for key in candidate_keys:
        if not key or key in seen:
            continue
        seen.add(key)
        for rec in lookup.get(key, []):
            score = safe_float(rec.get("priority_score"), 0.0)
            if normalize_text(rec.get("buy_signal_30pct", "")).upper() == "BUY":
                score += 100000.0
            if key == normalize_article(rec.get("comparison_article_norm", "")):
                score += 1000.0
            elif key == normalize_article(rec.get("watch_article_norm", "")):
                score += 500.0
            if score > best_score:
                best = rec
                best_score = score
    return best


def apply_hot_watchlist(df: pd.DataFrame | None, hot_df: pd.DataFrame | None, tab_label: str = "") -> pd.DataFrame | None:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return df
    lookup = build_hot_watchlist_lookup(hot_df, tab_label=tab_label)
    if not lookup:
        return df
    work = df.copy()
    matches = [pick_hot_watch_rec(row, lookup) for _, row in work.iterrows()]
    work["hot_flag"] = [bool(m) for m in matches]
    work["hot_sales_per_month"] = [safe_float((m or {}).get("sales_per_month"), 0.0) for m in matches]
    work["hot_priority_score"] = [safe_float((m or {}).get("priority_score"), 0.0) for m in matches]
    work["hot_abc_class"] = [normalize_text((m or {}).get("abc_class", "")) for m in matches]
    work["hot_velocity_band"] = [normalize_text((m or {}).get("velocity_band", "")) for m in matches]
    work["hot_action_today"] = [normalize_text((m or {}).get("action_today", "")) for m in matches]
    work["hot_buy_signal"] = [normalize_text((m or {}).get("buy_signal_30pct", "")) for m in matches]
    work["hot_best_supplier"] = [normalize_text((m or {}).get("best_supplier", "")) for m in matches]
    work["hot_best_supplier_gap_pct"] = [safe_float((m or {}).get("best_supplier_gap_pct"), 0.0) for m in matches]
    work["hot_watch_article"] = [normalize_text((m or {}).get("watch_article", "")) for m in matches]
    return work




def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Watchlist")
    return out.getvalue()

def hot_watchlist_summary_text() -> str:
    hot_df = st.session_state.get("hot_items_df")
    if not isinstance(hot_df, pd.DataFrame) or hot_df.empty:
        return "watchlist не загружен"
    buy_count = int((hot_df.get("buy_signal_30pct", pd.Series(dtype=object)).fillna("").map(normalize_text).str.upper() == "BUY").sum())
    ab_count = int(hot_df.get("abc_class", pd.Series(dtype=object)).fillna("").map(normalize_text).isin(["A", "B"]).sum())
    return f"Ходовых: {len(hot_df)} • сильный спрос: {ab_count} • можно брать: {buy_count}"


def hot_supplier_note(row: pd.Series | dict | None, best: dict | None, threshold_pct: float = 35.0) -> tuple[str, str]:
    help_text = "Товар ходовой → товар хорошо продавался за выбранный период"
    if not best:
        help_text += f"\nСейчас брать невыгодно → нет поставщика с ценой минимум на {threshold_pct:.0f}% ниже нашей цены"
        return "Сейчас брать невыгодно", help_text

    source = normalize_text((best or {}).get("source", ""))
    delta_pct = safe_float((best or {}).get("delta_percent"), 0.0)
    if delta_pct >= float(threshold_pct):
        action_text = f"Сейчас можно брать у {source}" if source else "Сейчас можно брать"
        help_text += f"\nСейчас можно брать → лучший поставщик сейчас минимум на {threshold_pct:.0f}% дешевле нашей цены"
        return action_text, help_text

    help_text += f"\nСейчас брать невыгодно → нет поставщика с ценой минимум на {threshold_pct:.0f}% ниже нашей цены"
    return "Сейчас брать невыгодно", help_text


