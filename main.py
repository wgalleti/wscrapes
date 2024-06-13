from datetime import datetime

import streamlit as st

from scrape.b3 import B3Scrape
from scrape.scoot_cepea import ScootCepeaScrape


def main():
    st.set_page_config(
        page_title="Leilo App Scrapes",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get Help': 'https://www.extremelycoolapp.com/help',
            'Report a bug': "https://www.extremelycoolapp.com/bug",
            'About': "# This is a header. This is an *extremely* cool app!"
        }
    )
    code = """
    <style>
        h1 {
            color: #CC7B30;
        }
    </style>
    """
    st.html(code)
    st.logo("logo.svg")
    scot = ScootCepeaScrape()
    b3 = B3Scrape()
    data_scot = scot.execute()
    data_b3 = b3.execute()

    today = datetime.now().strftime("%d/%m/%Y %H:%M")

    st.title(f'Dados Scot em {today}')
    columns = st.columns(len(data_scot))
    for i, title in enumerate(data_scot):
        with columns[i]:
            st.header(title)
            st.dataframe(data_scot[title], hide_index=True, use_container_width=True)

    st.title(f'Dados Notícias Agricolas em {today}')
    columns = st.columns(len(data_b3))
    for i, title in enumerate(data_b3):
        with columns[i]:
            st.header(title)
            st.dataframe(data_b3[title], hide_index=True, use_container_width=True)


if __name__ == '__main__':
    main()
