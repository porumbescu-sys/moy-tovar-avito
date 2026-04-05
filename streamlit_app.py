from __future__ import annotations

import html
import json
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
DEFAULT_DISCOUNT_1 = 12.0
DEFAULT_DISCOUNT_2 = 20.0

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



COLOR_KEYWORDS = [
    ("желтый", "желтый"),
    ("yellow", "желтый"),
    ("yello", "желтый"),
    ("cyan", "голубой"),
    ("голубой", "голубой"),
    ("синий", "синий"),
    ("blue", "синий"),
    ("magenta", "пурпурный"),
    ("пурпур", "пурпурный"),
    ("фиолет", "пурпурный"),
    ("purple", "пурпурный"),
    ("red", "красный"),
    ("красный", "красный"),
    ("black", "черный"),
    ("черный", "черный"),
    ("чёрный", "черный"),
    ("grey", "серый"),
    ("gray", "серый"),
    ("серый", "серый"),
    ("green", "зеленый"),
    ("зел", "зеленый"),
]

def init_state() -> None:
    defaults = {
        "catalog_df": None,
        "catalog_name": "ещё не загружен",
        "search_input": "",
        "submitted_query": "",
        "last_result": None,
        "price_mode": "-12%",
        "custom_discount": 15.0,
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


def current_discount(price_mode: str, custom_discount: float) -> float:
    if price_mode == "-12%":
        return DEFAULT_DISCOUNT_1
    if price_mode == "-20%":
        return DEFAULT_DISCOUNT_2
    return max(0.0, float(custom_discount))


def current_price_label(price_mode: str, custom_discount: float) -> str:
    disc = current_discount(price_mode, custom_discount)
    if disc.is_integer():
        return f"Цена -{int(disc)}%"
    return f"Цена -{str(round(disc, 2)).replace('.', ',')}%"


def get_selected_price_raw(row: pd.Series, price_mode: str, round100: bool, custom_discount: float) -> float:
    disc = current_discount(price_mode, custom_discount)
    value = float(row["sale_price"]) * (1 - disc / 100)
    if round100:
        return float(round_up_to_100(value))
    return float(round(value, 2))


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


def split_query_parts(query: str) -> list[str]:
    return [normalize_text(x) for x in re.split(r"[\n,;]+", query) if normalize_text(x)]


def detect_color(name: str) -> str:
    low = normalize_text(name).lower()
    for needle, label in COLOR_KEYWORDS:
        if needle in low:
            return label
    return ""


def fmt_ruble_template(value: float | int) -> str:
    return f"{int(round(float(value))):,}".replace(",", " ") + " руб."


def is_available(row: pd.Series) -> bool:
    try:
        return float(row.get("free_qty", 0)) > 0
    except Exception:
        return False


def build_offer_template(df: pd.DataFrame, query: str, round100: bool) -> str:
    lines: list[str] = []
    for part in split_query_parts(query):
        article_norm = normalize_article(part)
        exact = df[df["article_norm"] == article_norm] if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if exact.empty:
            lines.append(f"{part} --- продан")
            continue
        row = exact.iloc[0]
        if not is_available(row):
            color = detect_color(str(row["name"]))
            prefix = f"{row['article']} {color}".strip()
            lines.append(f"{prefix} --- продан")
            continue
        color = detect_color(str(row["name"]))
        prefix = f"{row['article']} {color}".strip()
        avito = float(row["sale_price"]) * (1 - DEFAULT_DISCOUNT_1 / 100)
        cash = avito * 0.90
        if round100:
            avito = round_up_to_100(avito)
            cash = round_up_to_100(cash)
        else:
            avito = round(avito)
            cash = round(cash)
        lines.append(f"{prefix} --- {fmt_ruble_template(avito)} - Авито / {fmt_ruble_template(cash)} за наличный расчет")
    return "\n\n".join(lines)


def build_selected_price_template(df: pd.DataFrame, query: str, price_mode: str, round100: bool, custom_discount: float) -> str:
    lines: list[str] = []
    for part in split_query_parts(query):
        article_norm = normalize_article(part)
        exact = df[df["article_norm"] == article_norm] if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if exact.empty:
            continue
        row = exact.iloc[0]
        if not is_available(row):
            continue
        selected_price = get_selected_price_raw(row, price_mode, round100, custom_discount)
        lines.append(f"{row['article']} {row['name']} --- {fmt_ruble_template(selected_price)}.")
    return "\n\n".join(lines)


def render_copy_big_button(text_value: str, button_label: str = "📋 Скопировать весь шаблон") -> None:
    escaped = json.dumps(text_value, ensure_ascii=False)
    html_block = f"""
    <div style='margin-top:8px;'>
      <button onclick='navigator.clipboard.writeText({escaped}).then(() => {{ this.innerText = "Скопировано"; setTimeout(() => this.innerText = {json.dumps(button_label, ensure_ascii=False)}, 1200); }})'
        style='border:none;background:#315efb;color:white;font-weight:800;border-radius:12px;padding:12px 16px;cursor:pointer;min-width:220px;'>
        {html.escape(button_label)}
      </button>
    </div>
    """
    components.html(html_block, height=58)


def perform_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    parts = split_query_parts(query)
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


def build_display_df(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float) -> pd.DataFrame:
    out = df.copy()
    out["selected_price"] = out.apply(lambda row: get_selected_price_raw(row, price_mode, round100, custom_discount), axis=1)
    label = current_price_label(price_mode, custom_discount)
    return pd.DataFrame(
        {
            "Артикул": out["article"],
            "Название": out["name"],
            "Производитель": out["brand"],
            "Свободно": out["free_qty"].map(fmt_qty),
            "Всего": out["total_qty"].map(fmt_qty),
            "Цена продажи": out["sale_price"].map(fmt_price),
            label: out["selected_price"].map(fmt_price),
        }
    )


def to_excel_bytes(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float) -> bytes:
    label = current_price_label(price_mode, custom_discount)
    export_df = pd.DataFrame(
        {
            "Артикул": df["article"],
            "Название": df["name"],
            "Производитель": df["brand"],
            "Свободно": df["free_qty"],
            "Всего": df["total_qty"],
            "Цена продажи": df["sale_price"],
            label: df.apply(lambda row: get_selected_price_raw(row, price_mode, round100, custom_discount), axis=1),
        }
    )
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Результаты")
    bio.seek(0)
    return bio.read()


def render_results_table(df: pd.DataFrame, price_mode: str, round100: bool, custom_discount: float) -> None:
    selected_label = current_price_label(price_mode, custom_discount)
    rows_html = []
    for _, row in df.iterrows():
        selected_raw = get_selected_price_raw(row, price_mode, round100, custom_discount)
        selected_fmt = fmt_price(selected_raw)
        rows_html.append(
            f"""
            <tr>
              <td><span class='article-pill'>{html.escape(str(row['article']))}</span></td>
              <td><div class='name-cell'>{html.escape(str(row['name']))}</div></td>
              <td>{html.escape(str(row['brand'] or ''))}</td>
              <td>{fmt_qty(row['free_qty'])}</td>
              <td>{fmt_qty(row['total_qty'])}</td>
              <td class='sale-col'>{fmt_price(row['sale_price'])}</td>
              <td class='selected-col'>{selected_fmt}</td>
              <td><button class='copy-btn' onclick="navigator.clipboard.writeText('{selected_fmt}').then(() => {{ this.innerText = 'Скопировано'; setTimeout(() => this.innerText = 'Копировать цену', 1200); }})">Копировать цену</button></td>
            </tr>
            """
        )
    table_html = f"""
    <!doctype html>
    <html><head><meta charset='utf-8'/>
    <style>
      body {{ margin:0; font-family: Inter, Arial, sans-serif; background: transparent; }}
      .wrap {{ background:white; border:1px solid #dbe5f1; border-radius:18px; overflow:hidden; }}
      table {{ width:100%; border-collapse:collapse; font-size:14px; }}
      thead th {{ background:#eef3fb; color:#334155; text-align:left; padding:14px; font-weight:800; border-bottom:1px solid #d7e1ef; }}
      tbody td {{ padding:14px; border-bottom:1px solid #e5edf6; vertical-align:top; color:#1e293b; }}
      tbody tr:last-child td {{ border-bottom:none; }}
      .article-pill {{ display:inline-block; padding:6px 10px; border-radius:999px; background:#edf2ff; color:#315efb; font-weight:800; }}
      .name-cell {{ font-weight:800; line-height:1.35; color:#1e293b; }}
      .sale-col {{ font-weight:800; }}
      .selected-col {{ background:#eef4ff; border-left:1px solid #c7d7ff; border-right:1px solid #c7d7ff; font-weight:900; color:#315efb; }}
      .copy-btn {{ border:none; background:#e9efff; color:#315efb; font-weight:800; border-radius:14px; padding:11px 14px; cursor:pointer; min-width:130px; }}
      .copy-btn:hover {{ background:#dce7ff; }}
    </style></head><body>
      <div class='wrap'><table>
        <thead><tr><th>Артикул</th><th>Название</th><th>Производитель</th><th>Свободно</th><th>Всего</th><th>Цена продажи</th><th>{html.escape(selected_label)}</th><th>Действие</th></tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table></div>
    </body></html>
    """
    height = min(max(170, 66 + len(df) * 74), 900)
    components.html(table_html, height=height, scrolling=True)


st.markdown(
    """
    <style>
    .stApp { background: #eef3f9; }
    header[data-testid="stHeader"] { background: rgba(0,0,0,0); }
    [data-testid="stToolbar"] { right: 1rem; top: .35rem; }
    [data-testid="stDecoration"] { display: none; }
    .block-container { max-width: 1560px; padding-top: 3.8rem; padding-bottom: 1.4rem; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0f172a 0%, #172554 100%); border-right: 1px solid rgba(255,255,255,.08); }
    [data-testid="stSidebar"] * { color: #e5ecff !important; }
    [data-testid="stSidebar"] .stFileUploader section { background: rgba(255,255,255,0.06) !important; border: 1px dashed rgba(255,255,255,0.25) !important; border-radius: 14px !important; }
    [data-testid="stSidebar"] .stNumberInput input,
    [data-testid="stSidebar"] .stTextInput input,
    [data-testid="stSidebar"] .stTextArea textarea {
        background: #ffffff !important;
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        caret-color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stNumberInput button,
    [data-testid="stSidebar"] .stNumberInput button * {
        color: #94a3b8 !important;
    }
    [data-testid="stSidebar"] .stNumberInput label,
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] .stCheckbox label {
        color: #e5ecff !important;
    }
    .topbar { background: linear-gradient(90deg, #0f172a 0%, #1d4ed8 100%); color: white; padding: 16px 18px; border-radius: 18px; margin-top: 0.4rem; margin-bottom: 10px; box-shadow: 0 12px 28px rgba(15, 23, 42, .18); }
    .topbar-grid { display:grid; grid-template-columns: 1.6fr 1fr 1fr 1fr; gap: 10px; align-items:center; }
    .brand-box { display:flex; gap:12px; align-items:center; }
    .logo { width:54px;height:54px;border-radius:14px;background:rgba(255,255,255,.14); display:flex;align-items:center;justify-content:center;font-size:26px;font-weight:700; }
    .brand-title { font-size: 24px; font-weight: 900; line-height: 1; }
    .brand-sub { font-size: 13px; opacity: .9; margin-top: 5px; }
    .stat-box { background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.12); border-radius: 14px; padding: 10px 12px; min-height: 70px; }
    .stat-cap { font-size: 12px; opacity: .82; margin-bottom: 4px; }
    .stat-val { font-size: 16px; font-weight: 800; }
    .toolbar, .result-wrap { background: white; border: 1px solid #dbe5f1; border-radius: 16px; padding: 12px 14px; margin-bottom: 10px; box-shadow: 0 6px 18px rgba(15, 23, 42, .05); }
    .toolbar-title, .section-title { font-size: 18px; font-weight: 900; color:#0f172a; margin-bottom:4px; }
    .toolbar-sub, .section-sub { font-size: 12px; color:#64748b; margin-bottom:10px; }
    .stDownloadButton > button, .stButton > button, button[kind="primary"] { border-radius: 10px !important; font-weight: 700 !important; min-height: 40px !important; }
    .stTextArea textarea, .stTextInput input, .stNumberInput input { border-radius: 10px !important; border: 1px solid #cbd5e1 !important; }
    .stRadio [role="radiogroup"] { gap: .4rem; }
    div[data-baseweb="radio"] label { background:#f8fafc; border:1px solid #dbe5f1; border-radius:10px; padding:8px 10px; margin-right:6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## 📦 Мой Товар")
    st.caption("Быстрый поиск по прайсу в локальном режиме")
    uploaded = st.file_uploader("Загрузить прайс", type=["xlsx", "xls", "xlsm", "csv"], help="Поддерживаются Excel и CSV")
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
            st.download_button("⬇️ Скачать demo_price.xlsx", sample_f.read(), file_name="demo_price.xlsx", use_container_width=True)
    st.divider()
    st.markdown("### ⚙️ Настройки")
    st.radio("Какая цена главная", ["Своя скидка", "-12%", "-20%"], key="price_mode", horizontal=False)
    st.number_input("Своя скидка, %", min_value=0.0, max_value=99.0, step=1.0, key="custom_discount", help="Используется только в режиме 'Своя скидка'")
    st.checkbox("Округлять вверх до 100", key="round100")
    st.caption("Кнопка копирования всегда берёт текущую выбранную цену.")

catalog_df = st.session_state.get("catalog_df")
file_name = st.session_state.get("catalog_name", "ещё не загружен")
rows_count = len(catalog_df) if isinstance(catalog_df, pd.DataFrame) else 0
price_mode = st.session_state.price_mode
round100 = st.session_state.round100
custom_discount = float(st.session_state.custom_discount)
price_label = current_price_label(price_mode, custom_discount)

st.markdown(f"""
<div class="topbar"><div class="topbar-grid">
<div class="brand-box"><div class="logo">📦</div><div><div class="brand-title">{APP_TITLE}</div><div class="brand-sub">Плотный рабочий интерфейс • поиск • цены • копирование</div></div></div>
<div class="stat-box"><div class="stat-cap">Текущий прайс</div><div class="stat-val">{html.escape(file_name)}</div></div>
<div class="stat-box"><div class="stat-cap">Строк в каталоге</div><div class="stat-val">{rows_count}</div></div>
<div class="stat-box"><div class="stat-cap">Режим цены</div><div class="stat-val">{html.escape(price_label)}{' • округл.' if round100 else ''}</div></div>
</div></div>
""", unsafe_allow_html=True)

st.markdown('<div class="toolbar">', unsafe_allow_html=True)
st.markdown('<div class="toolbar-title">Поиск товара</div><div class="toolbar-sub">Можно искать по одному или нескольким артикулам, а также по части названия или бренда.</div>', unsafe_allow_html=True)

with st.form("search_form", clear_on_submit=False):
    search_value = st.text_area("Поисковый запрос", value=st.session_state.search_input, placeholder="Например:\n006R01380\n106R00646\nили Xerox 700", height=90, label_visibility="collapsed")
    c1, c2, c3 = st.columns([1, 1, 2.4])
    find_clicked = c1.form_submit_button("🔎 Найти", use_container_width=True, type="primary")
    clear_clicked = c2.form_submit_button("🧹 Очистить", use_container_width=True)
    c3.markdown("<div style='padding-top:9px;color:#64748b;font-size:12px;'>Выберите режим цены слева. Если активна 'Своя скидка', приложение будет считать цену по вашему проценту и копировать именно её.</div>", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

if clear_clicked:
    st.session_state.search_input = ""
    st.session_state.submitted_query = ""
    st.session_state.last_result = None
    st.rerun()
if find_clicked:
    st.session_state.search_input = search_value
    st.session_state.submitted_query = search_value
    st.session_state.last_result = perform_search(st.session_state.catalog_df, search_value) if isinstance(st.session_state.catalog_df, pd.DataFrame) else None

current_df = st.session_state.catalog_df
submitted_query = st.session_state.submitted_query
result_df = st.session_state.last_result

st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Результаты</div><div class="section-sub">Показываю таблицу в стиле первой картинки: продажа, выбранная цена и кнопка копирования по строке.</div>', unsafe_allow_html=True)

if current_df is None:
    st.info("Сначала загрузите прайс в левой панели 👈")
elif not submitted_query.strip():
    st.info("Введите артикул или название и нажмите **Найти**.")
elif result_df is None or result_df.empty:
    st.warning("Ничего не найдено. Попробуйте другой артикул, бренд или часть названия.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Найдено", len(result_df))
    m2.metric("Цена", price_label)
    m3.metric("Округление", "вкл" if round100 else "выкл")
    m4.metric("Каталог", len(current_df))
    render_results_table(result_df.head(200), price_mode, round100, custom_discount)
    with st.expander("Показать техническую таблицу"):
        st.dataframe(build_display_df(result_df, price_mode, round100, custom_discount), use_container_width=True, hide_index=True, height=300)
    st.download_button("⬇️ Скачать найденное в Excel", to_excel_bytes(result_df, price_mode, round100, custom_discount), file_name="moy_tovar_results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
st.markdown("""<div class="section-title">Шаблон для Авито / сообщения клиенту</div><div class="section-sub">Для каждого введённого артикула собираю строку в формате: цена Авито = продажа -12%, цена за наличный расчёт = ещё -10% от цены Авито. Если артикула нет в прайсе — пишу «продан». Округление из левой панели применяется и здесь.</div>""", unsafe_allow_html=True)

if current_df is None:
    st.info("Сначала загрузите прайс в левой панели 👈")
elif not submitted_query.strip():
    st.info("Введите артикулы через Enter или запятую, затем нажмите **Найти**.")
else:
    template_text = build_offer_template(current_df, submitted_query, round100)
    line_count = len(split_query_parts(submitted_query))
    c1, c2 = st.columns([1, 4])
    c1.metric("Строк в шаблоне", line_count)
    c2.markdown("<div style='padding-top:30px;color:#64748b;font-size:13px;'>Ниже готовый текст. Округление до 100 и наличие по колонке «Свободно» применяются и здесь.</div>", unsafe_allow_html=True)
    st.text_area("Готовый шаблон", value=template_text, height=min(360, max(150, 52 + line_count * 42)), key="offer_template_text")
    render_copy_big_button(template_text)

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="result-wrap">', unsafe_allow_html=True)
st.markdown(f"""<div class="section-title">Шаблон: артикул + название + выбранная цена</div><div class="section-sub">Формат: Артикул + название --- цена. Цена берётся из выбранного режима слева ({html.escape(price_label)}). Если по колонке «Свободно» товара нет в наличии, он во второй шаблон не попадает. Округление из левой панели применяется и здесь.</div>""", unsafe_allow_html=True)

if current_df is None:
    st.info("Сначала загрузите прайс в левой панели 👈")
elif not submitted_query.strip():
    st.info("Введите артикулы через Enter или запятую, затем нажмите **Найти**.")
else:
    second_template_text = build_selected_price_template(current_df, submitted_query, price_mode, round100, custom_discount)
    if second_template_text.strip():
        second_line_count = len([x for x in second_template_text.split("\n\n") if x.strip()])
        c1, c2 = st.columns([1, 4])
        c1.metric("Строк во 2 шаблоне", second_line_count)
        c2.markdown("<div style='padding-top:30px;color:#64748b;font-size:13px;'>Во второй шаблон попадают только найденные позиции, которые есть в наличии.</div>", unsafe_allow_html=True)
        st.text_area("Готовый шаблон 2", value=second_template_text, height=min(360, max(150, 52 + second_line_count * 42)), key="selected_price_template_text")
        render_copy_big_button(second_template_text, "📋 Скопировать 2 шаблон")
    else:
        st.info("Во втором шаблоне нечего показывать: найденных позиций в наличии нет.")

st.markdown('</div>', unsafe_allow_html=True)
