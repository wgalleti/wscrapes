import re
from datetime import timedelta, datetime

import requests
from bs4 import BeautifulSoup

class ScrapeBase:
    SATURDAY_WEEK_DAY = 5
    TWO_GROUPS_EXPRESSION = r"^(\w{2})\s(.+)$"
    OUTPUT_CSV_DIR = "output"
    HEADER_DEFAULT = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    def __init__(self):
        self.soup = None

    def get_previous_weekday(self, date):
        """Retorna o dia útil anterior à data fornecida."""
        while date.weekday() >= self.SATURDAY_WEEK_DAY:
            date -= timedelta(days=1)
        return date

    def split_strings(self, text):
        """Expressão regular para verificar se a primeira palavra tem dois caracteres."""

        # remove depois ( informação desnecessária
        text = text.split("(")[0].replace(" - ", " ")
        match = re.match(self.TWO_GROUPS_EXPRESSION, text)
        if match:
            return [match.group(1), match.group(2)]

        return text.split()

    def save_dataframe_to_csv(self, df, filename):
        """Salva um DataFrame em um arquivo CSV."""
        current_time = datetime.now().strftime("%Y-%m-%d")
        save_file_name = f"{self.OUTPUT_CSV_DIR}/{current_time}-{filename}"
        df.to_csv(save_file_name, index=False)
        return save_file_name

    def fetch_page_content(self, url, headers=None):
        """Faz a requisição HTTP para obter o conteúdo da página e retorna o BeautifulSoup"""
        if headers is None:
            headers = self.HEADER_DEFAULT

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            self.soup = BeautifulSoup(response.content, "html.parser")
        else:
            self.soup = None
            print(f"Fail to process page {url}: {response.status_code}")
