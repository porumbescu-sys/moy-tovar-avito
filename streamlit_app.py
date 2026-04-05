import io
import math
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title='Мой Товар', page_icon='📦', layout='wide')

APP_TITLE = '📦 Мой Товар'
PRICE_COL_CANDIDATES = ['Цена', 'Цена продажи', 'Продажа', 'Розница', 'ЦенаПродажи']
SKU_CANDIDATES = ['Артикул', 'артикул', 'SKU', 'Код']
NAME_CANDIDATES = ['Номенклатура', 'Название', 'Наименование', 'Товар']
BRAND_CANDIDATES = ['Номенклатура.Производитель', 'Производитель', 'Бренд', 'Марка']
FREE_CANDIDATES = ['Свободно', 'Остаток', 'Свободный остаток']
TOTAL_CANDIDATES = ['Всего', 'Всего на складе', 'Остаток общий']


def find_col(df: pd.DataFrame, candidates: list[str]):
    lowered = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        if c.strip().lower() in lowered:
            return lowered[c.strip().lower()]
    return None


def round_up_to_100(value):
    if pd.isna(value):
        return value
    try:
        v = float(value)
    except Exception:
        return value
    return int(math.ceil(v / 100.0) * 100)


def fmt_price(value):
    if pd.isna(value):
        return ''
    try:
        return f"{float(value):,.0f}".replace(',', ' ')
    except Exception:
        return str(value)


@st.cache_data(show_spinner=False)
def load_file(uploaded_name: str, file_bytes: bytes) -> pd.DataFrame:
    suffix = Path(uploaded_name).suffix.lower()
    bio = io.BytesIO(file_bytes)
    if suffix in ['.xlsx', '.xlsm', '.xls']:
        return pd.read_excel(bio)
    return pd.read_csv(bio)


def enrich_df(df: pd.DataFrame, price_mode: str, round100: bool) -> tuple[pd.DataFrame, dict]:
    sku_col = find_col(df, SKU_CANDIDATES)
    name_col = find_col(df, NAME_CANDIDATES)
    brand_col = find_col(df, BRAND_CANDIDATES)
    free_col = find_col(df, FREE_CANDIDATES)
    total_col = find_col(df, TOTAL_CANDIDATES)
    price_col = find_col(df, PRICE_COL_CANDIDATES)

    if not price_col:
        raise ValueError('Не нашёл колонку с ценой. Ожидаю что-то вроде: Цена / Цена продажи / Продажа.')

    out = df.copy()
    out[price_col] = pd.to_numeric(out[price_col], errors='coerce')
    out['Цена -12%'] = out[price_col] * 0.88
    out['Цена -20%'] = out[price_col] * 0.80

    selected_source = {
        'Продажа': price_col,
        '-12%': 'Цена -12%',
        '-20%': 'Цена -20%',
    }[price_mode]
    out['Выбранная цена'] = out[selected_source]
    if round100:
        out['Выбранная цена'] = out['Выбранная цена'].apply(round_up_to_100)

    display = pd.DataFrame()
    if sku_col:
        display['Артикул'] = out[sku_col]
    if name_col:
        display['Название'] = out[name_col]
    if brand_col:
        display['Производитель'] = out[brand_col]
    if free_col:
        display['Свободно'] = out[free_col]
    if total_col:
        display['Всего'] = out[total_col]
    display['Цена продажи'] = out[price_col]
    display['Цена -12%'] = out['Цена -12%']
    display['Цена -20%'] = out['Цена -20%']
    display['Выбранная цена'] = out['Выбранная цена']

    meta = {
        'sku_col': sku_col,
        'name_col': name_col,
        'brand_col': brand_col,
        'free_col': free_col,
        'total_col': total_col,
        'price_col': price_col,
    }
    return display, meta


def filter_df(display: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query.strip():
        return display
    parts = [p.strip().lower() for p in query.replace('\n', ',').split(',') if p.strip()]
    if not parts:
        return display
    searchable_cols = [c for c in ['Артикул', 'Название', 'Производитель'] if c in display.columns]
    if not searchable_cols:
        return display
    mask = pd.Series(False, index=display.index)
    for p in parts:
        part_mask = pd.Series(False, index=display.index)
        for c in searchable_cols:
            part_mask = part_mask | display[c].astype(str).str.lower().str.contains(p, na=False)
        mask = mask | part_mask
    return display[mask]


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Результаты')
    bio.seek(0)
    return bio.read()


st.markdown(
    """
    <style>
    .big-title {font-size: 2rem; font-weight: 700; margin-bottom: 0.2rem;}
    .subtle {color: #6b7280; margin-bottom: 1rem;}
    .price-box {padding: 0.8rem 1rem; border: 1px solid #e5e7eb; border-radius: 14px; background: #fafafa;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(f'<div class="big-title">{APP_TITLE}</div>', unsafe_allow_html=True)
st.markdown('<div class="subtle">Поиск по прайсу: артикул, название, производитель. Выбор цены, округление вверх до 100 и выгрузка результата.</div>', unsafe_allow_html=True)

left, right = st.columns([1.1, 2])
with left:
    uploaded = st.file_uploader('Загрузите прайс Excel/CSV', type=['xlsx', 'xls', 'xlsm', 'csv'])
    price_mode = st.radio('Какую цену показывать и использовать', ['Продажа', '-12%', '-20%'], horizontal=True)
    round100 = st.checkbox('Округлять выбранную цену вверх до 100', value=False)
    query = st.text_area('Поиск', placeholder='Например: C13S015637\nили несколько артикулов через запятую')

    sample_path = Path(__file__).parent / 'data' / 'demo_price.xlsx'
    if sample_path.exists():
        with open(sample_path, 'rb') as f:
            st.download_button('Скачать demo_price.xlsx', f.read(), file_name='demo_price.xlsx')

if uploaded is None:
    st.info('Загрузите прайс, и таблица появится справа 👉')
    st.stop()

try:
    raw_df = load_file(uploaded.name, uploaded.getvalue())
    display_df, meta = enrich_df(raw_df, price_mode, round100)
    filtered = filter_df(display_df, query)
except Exception as e:
    st.error(f'Ошибка обработки файла: {e}')
    st.stop()

with right:
    top1, top2, top3 = st.columns(3)
    top1.metric('Всего строк', len(display_df))
    top2.metric('Найдено', len(filtered))
    top3.metric('Режим цены', price_mode)

    show_cols = [c for c in filtered.columns if c != 'Выбранная цена'] + ['Выбранная цена']
    fmt_cols = [c for c in ['Цена продажи', 'Цена -12%', 'Цена -20%', 'Выбранная цена'] if c in filtered.columns]
    styled = filtered[show_cols].copy()
    for c in fmt_cols:
        styled[c] = styled[c].apply(fmt_price)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    if len(filtered) == 1:
        val = filtered.iloc[0]['Выбранная цена']
        st.markdown('<div class="price-box">', unsafe_allow_html=True)
        st.write(f'**Готовая цена для копирования:** `{fmt_price(val)}`')
        st.caption('Выше показана цена с учётом текущего режима и округления.')
        st.markdown('</div>', unsafe_allow_html=True)

    export_bytes = to_excel_bytes(filtered)
    st.download_button(
        'Скачать найденное в Excel',
        export_bytes,
        file_name='moy_tovar_results.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )

st.caption('Совет: если ищете один товар и нужна готовая цена, удобнее вводить один артикул — тогда цена для копирования будет показана отдельно.')
