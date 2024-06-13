import pandas as pd

from scrape.base import ScrapeBase


class B3Scrape(ScrapeBase):
    MONTH_TRANSLATION = {
        "Janeiro": "January",
        "Fevereiro": "February",
        "Março": "March",
        "Abril": "April",
        "Maio": "May",
        "Junho": "June",
        "Julho": "July",
        "Agosto": "August",
        "Setembro": "September",
        "Outubro": "October",
        "Novembro": "November",
        "Dezembro": "December",
    }

    def __init__(self):
        super().__init__()
        self.scrapes = {}
        self.params = {
            "Boi Gordo": {
                "url": "https://www.noticiasagricolas.com.br/cotacoes/boi-gordo/boi-gordo-b3-prego-regular",
                "file_name": "boi_gordo_b3.csv",
            },
            "Dolar": {
                "url": "https://www.noticiasagricolas.com.br/cotacoes/mercado-financeiro/dolar-b3",
                "file_name": "dolar_b3.csv",
            },
        }

        self.data_frames = {}

    def execute(self) -> dict:
        def format_currency(value):
            return f'{value:,.4f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

        def translate(date_str):
            dates = date_str.split("/")
            months_keys = self.MONTH_TRANSLATION.keys()
            for key, value in enumerate(dates):
                if value in months_keys:
                    dates[key] = self.MONTH_TRANSLATION[dates[key]]
                    break

            return "/".join(dates)

        def transform_to_last_day_of_month(date_str):
            return (
                pd.to_datetime(translate(date_str), format="%B/%Y")
                + pd.offsets.MonthEnd(0)
            ).date()

        def transform_to_first_day_of_month(date_str):
            full_date_str = translate(date_str).replace("/24", "/2024")
            return (
                pd.to_datetime(full_date_str, format="%B/%Y") + pd.offsets.MonthBegin(0)
            ).date()

        transform_date_params = {
            "Boi Gordo": transform_to_last_day_of_month,
            "Dolar": transform_to_first_day_of_month,
        }

        for key in self.params.keys():
            url = self.params[key].get("url")
            self.fetch_page_content(url)

            if self.soup:
                table = self.soup.find("div", class_="table-content").find(
                    "table", class_="cot-fisicas"
                )
                if table:
                    df = pd.read_html(str(table), decimal=",", thousands=".")[0]

                    df.columns = ["Mês", "Valor", "Variacao"]
                    df = df[["Mês", "Valor"]]

                    transform_date = transform_date_params.get(key, None)

                    if transform_date:
                        df["Mês"] = df["Mês"].apply(transform_date)

                    if key == "Dolar":
                        df["Valor"] = df["Valor"].apply(lambda x: float(x) / 1000)

                    df["Valor"] = df["Valor"].apply(format_currency)

                    file = self.save_dataframe_to_csv(
                        df, self.params[key].get("file_name")
                    )
                    self.scrapes[key] = {"df": df, "file": file}
                else:
                    print("Table not found")

        for key, value in self.scrapes.items():
            self.data_frames[key] = value.get("df", None)

        return self.data_frames
