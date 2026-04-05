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
        "search_query": "",
        "submitted_query": "",
        "last_result": None,
        "price_mode": "-12%",
        "round100": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


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
        raise ValueError(
            "Не удалось определить обязательные колонки: " + ", ".join(missing)
        )

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
        val = float(row["sale_price"])
    elif price_mode == "-20%":
        val = float(row["price_20"])
    else:
        val = float(row["price_12"])
    return float(round_up_to_100(val) if round100 else round(val, 2))



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
        contains = df[contains_mask & ~df.index.isin(used_indices)].head(30)
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
    out["selected_price"] = out.apply(
        lambda row: get_selected_price_raw(row, price_mode, round100), axis=1
    )
    show = pd.DataFrame(
        {
            "Артикул": out["article"],
            "Название": out["name"],
            "Производитель": out["brand"],
            "Свободно": out["free_qty"].map(fmt_qty),
            "Всего": out["total_qty"].map(fmt_qty),
            "Цена продажи": out["sale_price"].map(fmt_price),
            f"Цена -{DEFAULT_DISCOUNT_1}%": out["price_12"].map(fmt_price),
            f"Цена -{DEFAULT_DISCOUNT_2}%": out["price_20"].map(fmt_price),
            "Выбранная цена": out["selected_price"].map(fmt_price),
        }
    )
    return show



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
            "Выбранная цена": df.apply(
                lambda row: get_selected_price_raw(row, price_mode, round100), axis=1
            ),
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
    <div style='margin-top:6px;'>
      <button id='btn-{key}' onclick="navigator.clipboard.writeText('{safe_text}').then(() => {{
        const b = document.getElementById('btn-{key}');
        const old = b.innerText;
        b.innerText = '✅ Скопировано';
        setTimeout(() => b.innerText = old, 1400);
      }})" 
      style="background:linear-gradient(135deg,#2563eb,#7c3aed);color:white;border:none;border-radius:12px;padding:10px 14px;font-weight:700;cursor:pointer;width:100%;">
      {safe_label}
      </button>
    </div>
    """
    components.html(html, height=58)


st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(180deg, #f4f7ff 0%, #f8fbff 100%);
    }
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
        max-width: 1450px;
    }
    .hero {
        background: linear-gradient(135deg, rgba(37,99,235,0.98), rgba(124,58,237,0.96));
        border-radius: 28px;
        padding: 28px 30px;
        color: white;
        box-shadow: 0 18px 50px rgba(37,99,235,0.22);
        margin-bottom: 18px;
    }
    .hero-grid {
        display: grid;
        grid-template-columns: 1.7fr 1fr;
        gap: 18px;
        align-items: center;
    }
    .logo-badge {
        width: 74px;
        height: 74px;
        border-radius: 22px;
        background: rgba(255,255,255,0.18);
        display:flex;
        align-items:center;
        justify-content:center;
        font-size: 34px;
        margin-bottom: 12px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.2);
    }
    .eyebrow { font-size: 13px; opacity: .9; letter-spacing: .4px; margin-bottom: 4px; }
    .hero h1 { font-size: 42px; line-height: 1.05; margin: 0 0 6px 0; }
    .hero p { font-size: 16px; line-height: 1.5; margin: 0; opacity: .96; }
    .hero-stat {
        background: rgba(255,255,255,0.14);
        border: 1px solid rgba(255,255,255,.14);
        border-radius: 18px;
        padding: 14px 16px;
        margin-bottom: 10px;
        backdrop-filter: blur(8px);
    }
    .hero-stat .label { font-size: 12px; opacity: .9; margin-bottom: 3px; }
    .hero-stat .value { font-size: 18px; font-weight: 800; }
    .panel {
        background: white;
        border: 1px solid #e9eefb;
        border-radius: 24px;
        padding: 20px;
        box-shadow: 0 8px 30px rgba(14, 30, 84, 0.05);
        height: 100%;
    }
    .panel-title { font-size: 23px; font-weight: 800; margin-bottom: 6px; color: #0f172a; }
    .panel-sub { color: #64748b; font-size: 14px; margin-bottom: 14px; }
    .result-card {
        background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
        border: 1px solid #e5ecfb;
        border-radius: 22px;
        padding: 16px;
        margin-bottom: 12px;
        box-shadow: 0 8px 24px rgba(15,23,42,.04);
    }
    .result-head { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }
    .result-article {
        display:inline-block; background:#eef4ff; color:#1d4ed8; border-radius:999px;
        padding:6px 12px; font-weight:800; font-size:13px; margin-bottom:8px;
    }
    .result-name { font-size:18px; font-weight:800; color:#0f172a; margin-bottom:4px; }
    .result-brand { color:#64748b; font-size:14px; }
    .price-pill {
        background: linear-gradient(135deg, #eff6ff, #f5f3ff);
        border: 1px solid #dbeafe;
        padding: 10px 14px;
        border-radius: 16px;
        min-width: 170px;
        text-align: right;
    }
    .price-pill .cap { font-size: 12px; color:#64748b; }
    .price-pill .num { font-size: 22px; font-weight: 900; color:#111827; }
    .mini-grid {
        display:grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap:10px; margin-top:12px;
    }
    .mini-box {
        background:#f8fbff; border:1px solid #e6eefb; border-radius:16px; padding:10px 12px;
    }
    .mini-cap { font-size:12px; color:#64748b; margin-bottom:4px; }
    .mini-val { font-size:16px; font-weight:800; color:#111827; }
    .help-box {
        background: linear-gradient(180deg, #f8fbff, #ffffff);
        border: 1px solid #e6eefc;
        border-radius: 20px;
        padding: 16px 18px;
    }
    .help-box ul { margin: 0; padding-left: 18px; }
    .help-box li { margin-bottom: 8px; color:#334155; }
    div[data-testid="stFileUploader"] section {
        border-radius: 18px;
        background: #f8fbff;
        border: 1px dashed #b8ccff;
    }
    button[kind="primary"] {
        border-radius: 14px !important;
        font-weight: 700 !important;
    }
    div[data-baseweb="radio"] label, div[data-testid="stCheckbox"] label {
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

catalog_df = st.session_state.get("catalog_df")
file_name = st.session_state.get("catalog_name", "ещё не загружен")
rows_count = len(catalog_df) if isinstance(catalog_df, pd.DataFrame) else 0

st.markdown(
    f"""
    <div class="hero">
      <div class="hero-grid">
        <div>
          <div class="logo-badge">📦</div>
          <div class="eyebrow">Удобный локальный прайс-поиск</div>
          <h1>{APP_TITLE}</h1>
          <p>Красивый и быстрый поиск как в рабочей программе: загружаете прайс, жмёте <b>Найти</b>, выбираете нужную цену, округляете при необходимости и копируете её в один клик.</p>
        </div>
        <div>
          <div class="hero-stat"><div class="label">📁 Текущий прайс</div><div class="value">{file_name}</div></div>
          <div class="hero-stat"><div class="label">🧾 Строк в каталоге</div><div class="value">{rows_count}</div></div>
          <div class="hero-stat"><div class="label">💸 Режимы цены</div><div class="value">Продажа / -{DEFAULT_DISCOUNT_1}% / -{DEFAULT_DISCOUNT_2}%</div></div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns([1.08, 1.72], gap="large")

with left:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">1. Загрузка прайса</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">Поддерживаются Excel и CSV. После загрузки каталог сразу готов к поиску.</div>', unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Выберите файл прайса",
        type=["xlsx", "xls", "xlsm", "csv"],
        label_visibility="collapsed",
    )

    if uploaded is not None:
        try:
            st.session_state.catalog_df = load_price_file(uploaded.name, uploaded.getvalue())
            st.session_state.catalog_name = uploaded.name
            catalog_df = st.session_state.catalog_df
            file_name = st.session_state.catalog_name
            rows_count = len(catalog_df)
            st.success(f"Прайс загружен: {uploaded.name} • строк: {rows_count}")
        except Exception as e:
            st.error(f"Ошибка загрузки: {e}")

    sample_path = Path(__file__).parent / "data" / "demo_price.xlsx"
    if sample_path.exists():
        with open(sample_path, "rb") as f:
            st.download_button(
                "⬇️ Скачать demo_price.xlsx",
                f.read(),
                file_name="demo_price.xlsx",
                use_container_width=True,
            )

    st.divider()
    st.markdown('<div class="panel-title">2. Цена и поиск</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">Сначала выберите режим цены, потом введите артикул и нажмите кнопку поиска.</div>', unsafe_allow_html=True)

    price_mode = st.radio(
        "Какую цену показывать и копировать",
        ["Продажа", "-12%", "-20%"],
        horizontal=True,
        key="price_mode",
    )
    round100 = st.checkbox(
        "Округлять выбранную цену вверх до 100",
        key="round100",
    )

    with st.form("search_form", clear_on_submit=False):
        search_query = st.text_area(
            "Поисковый запрос",
            value=st.session_state.search_query,
            placeholder="Например:\n006R01380\n106R00646\nили Xerox 700",
            height=140,
            label_visibility="collapsed",
        )
        b1, b2 = st.columns(2)
        search_pressed = b1.form_submit_button("🔎 Найти", use_container_width=True, type="primary")
        clear_pressed = b2.form_submit_button("🧹 Очистить", use_container_width=True)

    if clear_pressed:
        st.session_state.search_query = ""
        st.session_state.submitted_query = ""
        st.session_state.last_result = None
        st.rerun()

    if search_pressed:
        st.session_state.search_query = search_query
        st.session_state.submitted_query = search_query
        if isinstance(st.session_state.catalog_df, pd.DataFrame):
            st.session_state.last_result = perform_search(st.session_state.catalog_df, search_query)
        else:
            st.session_state.last_result = None

    st.markdown('<div class="help-box"><b>Подсказки</b><ul><li>Сначала ищется точное совпадение по артикулу.</li><li>Потом показываются похожие результаты по названию и бренду.</li><li>Кнопка копирует именно ту цену, которая выбрана сверху.</li><li>Если изменить режим цены или округление, таблица и кнопки обновятся автоматически.</li></ul></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Результаты</div>', unsafe_allow_html=True)

    current_df = st.session_state.catalog_df
    submitted_query = st.session_state.submitted_query
    price_mode = st.session_state.price_mode
    round100 = st.session_state.round100
    result_df = st.session_state.last_result

    if current_df is None:
        st.info("Сначала загрузите прайс слева 👈")
    elif not submitted_query.strip():
        st.info("Введите артикул или название и нажмите **Найти** 👈")
    elif result_df is None or result_df.empty:
        st.warning("Ничего не найдено. Попробуйте другой артикул или часть названия.")
    else:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Найдено", len(result_df))
        m2.metric("Режим цены", price_mode)
        m3.metric("Округление", "вкл" if round100 else "выкл")
        m4.metric("Каталог", len(current_df))

        display_df = build_display_df(result_df, price_mode, round100)
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=320)

        export_bytes = to_excel_bytes(result_df, price_mode, round100)
        st.download_button(
            "⬇️ Скачать найденное в Excel",
            export_bytes,
            file_name="moy_tovar_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        st.markdown("### Быстрое копирование")
        st.caption("Ниже каждая позиция с крупной ценой и кнопкой копирования — как в рабочем интерфейсе, а не просто таблица.")
        for idx, row in result_df.head(50).iterrows():
            selected_raw = get_selected_price_raw(row, price_mode, round100)
            selected_fmt = fmt_price(selected_raw)
            sale_fmt = fmt_price(row["sale_price"])
            p12_fmt = fmt_price(row["price_12"])
            p20_fmt = fmt_price(row["price_20"])
            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-head">
                        <div>
                            <div class="result-article">{row['article']}</div>
                            <div class="result-name">{row['name']}</div>
                            <div class="result-brand">🏷️ {row['brand'] or 'Без производителя'}</div>
                        </div>
                        <div class="price-pill">
                            <div class="cap">Выбранная цена {price_mode}</div>
                            <div class="num">{selected_fmt}</div>
                        </div>
                    </div>
                    <div class="mini-grid">
                        <div class="mini-box"><div class="mini-cap">Свободно</div><div class="mini-val">{fmt_qty(row['free_qty'])}</div></div>
                        <div class="mini-box"><div class="mini-cap">Всего</div><div class="mini-val">{fmt_qty(row['total_qty'])}</div></div>
                        <div class="mini-box"><div class="mini-cap">Цена продажи</div><div class="mini-val">{sale_fmt}</div></div>
                        <div class="mini-box"><div class="mini-cap">Цена -{DEFAULT_DISCOUNT_1}% / -{DEFAULT_DISCOUNT_2}%</div><div class="mini-val">{p12_fmt} / {p20_fmt}</div></div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            render_copy_button(selected_fmt, key=f"copy_{idx}")
    st.markdown('</div>', unsafe_allow_html=True)
