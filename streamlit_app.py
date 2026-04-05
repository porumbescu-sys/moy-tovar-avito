from __future__ import annotations

import io
import math
import re
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Мой Товар", page_icon="📦", layout="wide")

APP_TITLE = "Мой Товар"
DEFAULT_DISCOUNT_1 = 12
DEFAULT_DISCOUNT_2 = 20

COLUMN_ALIASES = {
    "article": ["Артикул", "артикул", "код", "sku", "артикл", "article"],
    "name": ["Номенклатура", "Наименование", "название", "товар", "name"],
    "brand": [
        "Номенклатура.Производитель",
        "Производитель",
        "бренд",
        "марка",
        "brand",
    ],
    "free_qty": ["Свободно", "Свободный остаток", "остаток", "наличие", "free"],
    "total_qty": ["Всего", "Количество", "всего на складе", "total"],
    "price": ["Цена", "Цена продажи", "Продажа", "розница", "price"],
}


def init_state() -> None:
    defaults = {
        "catalog_df": None,
        "catalog_name": "ещё не загружен",
        "search_input": "",
        "submitted_query": "",
        "last_result": None,
        "price_mode": "-12%",
        "round100": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def normalize_article(value: object) -> str:
    text = normalize_text(value)
    return re.sub(r"[^A-Za-zА-Яа-я0-9]", "", text).upper()


def find_column(columns: list[str], candidates: list[str]) -> Optional[str]:
    lower_map = {str(col).strip().lower(): col for col in columns}
    for candidate in candidates:
        col = lower_map.get(candidate.strip().lower())
        if col is not None:
            return col
    for candidate in candidates:
        c_low = candidate.strip().lower()
        for original in columns:
            o_low = str(original).strip().lower()
            if c_low in o_low or o_low in c_low:
                return original
    return None


def detect_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {key: find_column(list(df.columns), aliases) for key, aliases in COLUMN_ALIASES.items()}


@st.cache_data(show_spinner=False)
def load_price_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    bio = io.BytesIO(file_bytes)
    if suffix == ".csv":
        try:
            raw = pd.read_csv(bio)
        except UnicodeDecodeError:
            bio.seek(0)
            raw = pd.read_csv(bio, encoding="cp1251")
    else:
        raw = pd.read_excel(bio)

    raw = raw.dropna(how="all")
    mapping = detect_columns(raw)
    required = ["article", "name", "price"]
    missing = [k for k in required if not mapping.get(k)]
    if missing:
        raise ValueError("Не удалось определить обязательные колонки: " + ", ".join(missing))

    data = pd.DataFrame()
    data["article"] = raw[mapping["article"]].map(normalize_text)
    data["article_norm"] = raw[mapping["article"]].map(normalize_article)
    data["name"] = raw[mapping["name"]].map(normalize_text)
    data["brand"] = raw[mapping["brand"]].map(normalize_text) if mapping.get("brand") else ""
    data["free_qty"] = (
        pd.to_numeric(raw[mapping["free_qty"]], errors="coerce").fillna(0)
        if mapping.get("free_qty")
        else 0
    )
    data["total_qty"] = (
        pd.to_numeric(raw[mapping["total_qty"]], errors="coerce").fillna(0)
        if mapping.get("total_qty")
        else 0
    )
    data["sale_price"] = pd.to_numeric(raw[mapping["price"]], errors="coerce")
    data = data.dropna(subset=["sale_price"])
    data = data[data["article_norm"] != ""]
    data = data.drop_duplicates(subset=["article_norm"], keep="first")

    data["sale_price"] = data["sale_price"].astype(float)
    data["price_12"] = data["sale_price"] * (1 - DEFAULT_DISCOUNT_1 / 100)
    data["price_20"] = data["sale_price"] * (1 - DEFAULT_DISCOUNT_2 / 100)
    data["search_blob"] = (
        data["article_norm"]
        + " "
        + data["article"].fillna("")
        + " "
        + data["name"].fillna("")
        + " "
        + data["brand"].fillna("")
    ).str.upper()
    return data.reset_index(drop=True)


def round_up_to_100(value: float) -> int:
    return int(math.ceil(float(value) / 100.0) * 100)


def get_selected_price_raw(row: pd.Series, price_mode: str, round100: bool) -> float:
    if price_mode == "Продажа":
        value = float(row["sale_price"])
    elif price_mode == "-20%":
        value = float(row["price_20"])
    else:
        value = float(row["price_12"])
    return float(round_up_to_100(value) if round100 else round(value, 2))


def fmt_price(value: float | int) -> str:
    if pd.isna(value):
        return ""
    value = float(value)
    if float(value).is_integer():
        return f"{int(value):,}".replace(",", " ")
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def fmt_qty(value: float | int) -> str:
    try:
        v = float(value)
    except Exception:
        return str(value)
    if v.is_integer():
        return str(int(v))
    return f"{v:,.2f}".replace(",", " ").replace(".", ",")


def perform_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    parts = [normalize_text(x) for x in re.split(r"[\n,;]+", query) if normalize_text(x)]
    if not parts:
        return df.iloc[0:0].copy()

    results = []
    used_indices = set()
    for part in parts:
        article_norm = normalize_article(part)
        exact = df[df["article_norm"] == article_norm]
        if not exact.empty:
            results.append(exact)
            used_indices.update(exact.index.tolist())
            continue
        contains_mask = df["search_blob"].str.contains(re.escape(part.upper()), na=False)
        contains = df[contains_mask & ~df.index.isin(used_indices)].head(50)
        if not contains.empty:
            results.append(contains)
            used_indices.update(contains.index.tolist())

    if not results:
        return df.iloc[0:0].copy()
    merged = pd.concat(results, ignore_index=False)
    merged = merged.drop_duplicates(subset=["article_norm"], keep="first")
    return merged.reset_index(drop=True)


def build_display_df(df: pd.DataFrame, price_mode: str, round100: bool) -> pd.DataFrame:
    out = df.copy()
    out["selected_price"] = out.apply(lambda row: get_selected_price_raw(row, price_mode, round100), axis=1)
    return pd.DataFrame(
        {
            "Артикул": out["article"],
            "Название": out["name"],
            "Производитель": out["brand"],
            "Свободно": out["free_qty"].map(fmt_qty),
            "Всего": out["total_qty"].map(fmt_qty),
            "Продажа": out["sale_price"].map(fmt_price),
            f"-{DEFAULT_DISCOUNT_1}%": out["price_12"].map(fmt_price),
            f"-{DEFAULT_DISCOUNT_2}%": out["price_20"].map(fmt_price),
            "Выбранная цена": out["selected_price"].map(fmt_price),
        }
    )


def to_excel_bytes(df: pd.DataFrame, price_mode: str, round100: bool) -> bytes:
    export_df = pd.DataFrame(
        {
            "Артикул": df["article"],
            "Название": df["name"],
            "Производитель": df["brand"],
            "Свободно": df["free_qty"],
            "Всего": df["total_qty"],
            "Цена продажи": df["sale_price"],
            f"Цена -{DEFAULT_DISCOUNT_1}%": df["price_12"],
            f"Цена -{DEFAULT_DISCOUNT_2}%": df["price_20"],
            "Выбранная цена": df.apply(lambda row: get_selected_price_raw(row, price_mode, round100), axis=1),
        }
    )
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Результаты")
    bio.seek(0)
    return bio.read()


def render_copy_button(text: str, key: str, label: str = "📋 Копировать цену") -> None:
    safe_text = str(text).replace("\\", "\\\\").replace("'", "\\'")
    safe_label = str(label).replace("<", "&lt;").replace(">", "&gt;")
    html = f"""
    <button id='btn-{key}' onclick="navigator.clipboard.writeText('{safe_text}').then(() => {{
      const b = document.getElementById('btn-{key}');
      const old = b.innerText;
      b.innerText = '✅ Скопировано';
      setTimeout(() => b.innerText = old, 1200);
    }})"
    style="background:#0f62fe;color:white;border:none;border-radius:10px;padding:10px 12px;font-weight:700;cursor:pointer;width:100%;">
    {safe_label}
    </button>
    """
    components.html(html, height=52)


st.markdown(
    """
    <style>
    .stApp { background: #eef3f9; }
    .block-container { max-width: 1560px; padding-top: 0.8rem; padding-bottom: 1.4rem; }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #172554 100%);
        border-right: 1px solid rgba(255,255,255,.08);
    }
    [data-testid="stSidebar"] * { color: #e5ecff !important; }
    [data-testid="stSidebar"] .stFileUploader section {
        background: rgba(255,255,255,0.06) !important;
        border: 1px dashed rgba(255,255,255,0.25) !important;
        border-radius: 14px !important;
    }
    .topbar {
        background: linear-gradient(90deg, #0f172a 0%, #1d4ed8 100%);
        color: white;
        padding: 16px 18px;
        border-radius: 18px;
        margin-bottom: 10px;
        box-shadow: 0 12px 28px rgba(15, 23, 42, .18);
    }
    .topbar-grid {
        display:grid;
        grid-template-columns: 1.6fr 1fr 1fr 1fr;
        gap: 10px;
        align-items:center;
    }
    .brand-box { display:flex; gap:12px; align-items:center; }
    .logo {
        width:54px;height:54px;border-radius:14px;background:rgba(255,255,255,.14);
        display:flex;align-items:center;justify-content:center;font-size:26px;font-weight:700;
    }
    .brand-title { font-size: 24px; font-weight: 900; line-height: 1; }
    .brand-sub { font-size: 13px; opacity: .9; margin-top: 5px; }
    .stat-box {
        background: rgba(255,255,255,.12);
        border: 1px solid rgba(255,255,255,.12);
        border-radius: 14px;
        padding: 10px 12px;
        min-height: 70px;
    }
    .stat-cap { font-size: 12px; opacity: .82; margin-bottom: 4px; }
    .stat-val { font-size: 16px; font-weight: 800; }
    .toolbar {
        background: white;
        border: 1px solid #dbe5f1;
        border-radius: 16px;
        padding: 12px 14px;
        margin-bottom: 10px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, .05);
    }
    .toolbar-title { font-size: 13px; font-weight: 800; color:#0f172a; margin-bottom: 3px; }
    .toolbar-sub { font-size: 12px; color:#64748b; }
    .result-wrap {
        background: white;
        border: 1px solid #dbe5f1;
        border-radius: 16px;
        padding: 12px 14px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, .05);
    }
    .result-card {
        background: #fbfdff;
        border: 1px solid #dbe7f5;
        border-radius: 14px;
        padding: 12px;
        margin-bottom: 10px;
    }
    .row-head {
        display:grid;
        grid-template-columns: 1.7fr .7fr;
        gap: 10px;
        align-items:start;
    }
    .article-pill {
        display:inline-block;
        background:#eff6ff;
        color:#1d4ed8;
        border:1px solid #bfdbfe;
        border-radius:999px;
        font-size:12px;
        font-weight:800;
        padding:4px 10px;
        margin-bottom:6px;
    }
    .name-line { font-size:16px; font-weight:800; color:#0f172a; margin-bottom:4px; }
    .brand-line { font-size:13px; color:#64748b; }
    .price-main {
        background: linear-gradient(135deg, #eff6ff, #eef2ff);
        border:1px solid #dbeafe;
        border-radius:14px;
        padding:10px 12px;
        text-align:right;
    }
    .price-cap { font-size:11px; color:#64748b; }
    .price-num { font-size:24px; font-weight:900; color:#111827; line-height:1.1; }
    .mini-grid {
        display:grid;
        grid-template-columns: repeat(5, minmax(100px,1fr));
        gap:8px;
        margin-top:10px;
    }
    .mini {
        border:1px solid #e5edf6;
        border-radius:12px;
        padding:8px 10px;
        background:white;
    }
    .mini-cap { font-size:11px; color:#64748b; margin-bottom:3px; }
    .mini-val { font-size:14px; font-weight:800; color:#111827; }
    .section-title { font-size: 18px; font-weight: 900; color: #0f172a; margin: 4px 0 10px 0; }
    .section-sub { font-size: 12px; color:#64748b; margin-bottom: 10px; }
    .stDownloadButton > button,
    .stButton > button,
    button[kind="primary"] {
        border-radius: 10px !important;
        font-weight: 700 !important;
        min-height: 40px !important;
    }
    .stTextArea textarea, .stTextInput input {
        border-radius: 10px !important;
        border: 1px solid #cbd5e1 !important;
    }
    .stRadio [role="radiogroup"] {
        gap: .4rem;
    }
    div[data-baseweb="radio"] label {
        background:#f8fafc; border:1px solid #dbe5f1; border-radius:10px; padding:8px 10px; margin-right:6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## 📦 Мой Товар")
    st.caption("Быстрый поиск по прайсу в локальном режиме")

    uploaded = st.file_uploader(
        "Загрузить прайс",
        type=["xlsx", "xls", "xlsm", "csv"],
        help="Поддерживаются Excel и CSV",
    )

    if uploaded is not None:
        try:
            st.session_state.catalog_df = load_price_file(uploaded.name, uploaded.getvalue())
            st.session_state.catalog_name = uploaded.name
            st.success(f"Загружен: {uploaded.name}")
        except Exception as exc:
            st.error(f"Ошибка: {exc}")

    sample_path = Path(__file__).parent / "data" / "demo_price.xlsx"
    if sample_path.exists():
        with open(sample_path, "rb") as sample_f:
            st.download_button(
                "⬇️ Скачать demo_price.xlsx",
                sample_f.read(),
                file_name="demo_price.xlsx",
                use_container_width=True,
            )

    st.divider()
    st.markdown("### ⚙️ Настройки")
    st.radio(
        "Какая цена главная",
        ["Продажа", "-12%", "-20%"],
        key="price_mode",
        horizontal=False,
    )
    st.checkbox("Округлять вверх до 100", key="round100")
    st.caption("Копирование и таблица всегда берут именно этот режим цены.")

catalog_df = st.session_state.get("catalog_df")
file_name = st.session_state.get("catalog_name", "ещё не загружен")
rows_count = len(catalog_df) if isinstance(catalog_df, pd.DataFrame) else 0
price_mode = st.session_state.price_mode
round100 = st.session_state.round100

st.markdown(
    f"""
    <div class="topbar">
      <div class="topbar-grid">
        <div class="brand-box">
          <div class="logo">📦</div>
          <div>
            <div class="brand-title">{APP_TITLE}</div>
            <div class="brand-sub">Плотный рабочий интерфейс • поиск • цены • копирование</div>
          </div>
        </div>
        <div class="stat-box"><div class="stat-cap">Текущий прайс</div><div class="stat-val">{file_name}</div></div>
        <div class="stat-box"><div class="stat-cap">Строк в каталоге</div><div class="stat-val">{rows_count}</div></div>
        <div class="stat-box"><div class="stat-cap">Режим цены</div><div class="stat-val">{price_mode}{' • округл.' if round100 else ''}</div></div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="toolbar">', unsafe_allow_html=True)
st.markdown('<div class="toolbar-title">Поиск товара</div><div class="toolbar-sub">Можно искать по одному или нескольким артикулам, а также по части названия или бренда.</div>', unsafe_allow_html=True)

with st.form("search_form", clear_on_submit=False):
    search_value = st.text_area(
        "Поисковый запрос",
        value=st.session_state.search_input,
        placeholder="Например:\n006R01380\n106R00646\nили Xerox 700",
        height=90,
        label_visibility="collapsed",
    )
    c1, c2, c3 = st.columns([1, 1, 2.4])
    find_clicked = c1.form_submit_button("🔎 Найти", use_container_width=True, type="primary")
    clear_clicked = c2.form_submit_button("🧹 Очистить", use_container_width=True)
    c3.markdown("<div style='padding-top:9px;color:#64748b;font-size:12px;'>Сначала загрузите прайс слева, затем ищите. После смены режима цены копирование обновляется автоматически.</div>", unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

if clear_clicked:
    st.session_state.search_input = ""
    st.session_state.submitted_query = ""
    st.session_state.last_result = None
    st.rerun()

if find_clicked:
    st.session_state.search_input = search_value
    st.session_state.submitted_query = search_value
    if isinstance(st.session_state.catalog_df, pd.DataFrame):
        st.session_state.last_result = perform_search(st.session_state.catalog_df, search_value)
    else:
        st.session_state.last_result = None

current_df = st.session_state.catalog_df
submitted_query = st.session_state.submitted_query
result_df = st.session_state.last_result

st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Результаты</div><div class="section-sub">Таблица для просмотра и карточки ниже для быстрого копирования выбранной цены.</div>', unsafe_allow_html=True)

if current_df is None:
    st.info("Сначала загрузите прайс в левой панели 👈")
elif not submitted_query.strip():
    st.info("Введите артикул или название и нажмите **Найти**.")
elif result_df is None or result_df.empty:
    st.warning("Ничего не найдено. Попробуйте другой артикул, бренд или часть названия.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Найдено", len(result_df))
    m2.metric("Цена", price_mode)
    m3.metric("Округление", "вкл" if round100 else "выкл")
    m4.metric("Каталог", len(current_df))

    display_df = build_display_df(result_df, price_mode, round100)
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=290)

    export_bytes = to_excel_bytes(result_df, price_mode, round100)
    st.download_button(
        "⬇️ Скачать найденное в Excel",
        export_bytes,
        file_name="moy_tovar_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )

    st.markdown("### Быстрое копирование")
    for idx, row in result_df.head(60).iterrows():
        selected_raw = get_selected_price_raw(row, price_mode, round100)
        selected_fmt = fmt_price(selected_raw)
        sale_fmt = fmt_price(row["sale_price"])
        p12_fmt = fmt_price(row["price_12"])
        p20_fmt = fmt_price(row["price_20"])

        st.markdown(
            f"""
            <div class="result-card">
              <div class="row-head">
                <div>
                  <div class="article-pill">{row['article']}</div>
                  <div class="name-line">{row['name']}</div>
                  <div class="brand-line">🏷️ {row['brand'] or 'Без производителя'}</div>
                </div>
                <div class="price-main">
                  <div class="price-cap">Выбранная цена {price_mode}</div>
                  <div class="price-num">{selected_fmt}</div>
                </div>
              </div>
              <div class="mini-grid">
                <div class="mini"><div class="mini-cap">Свободно</div><div class="mini-val">{fmt_qty(row['free_qty'])}</div></div>
                <div class="mini"><div class="mini-cap">Всего</div><div class="mini-val">{fmt_qty(row['total_qty'])}</div></div>
                <div class="mini"><div class="mini-cap">Продажа</div><div class="mini-val">{sale_fmt}</div></div>
                <div class="mini"><div class="mini-cap">-{DEFAULT_DISCOUNT_1}%</div><div class="mini-val">{p12_fmt}</div></div>
                <div class="mini"><div class="mini-cap">-{DEFAULT_DISCOUNT_2}%</div><div class="mini-val">{p20_fmt}</div></div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        render_copy_button(selected_fmt, key=f"copy_{idx}")

st.markdown('</div>', unsafe_allow_html=True)
