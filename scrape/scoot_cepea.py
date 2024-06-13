import re
from datetime import datetime, timedelta
from scrape.base import ScrapeBase
import pandas as pd


class ScootCepeaScrape(ScrapeBase):
    STATES = [
        {
            "acronym": "AC",
            "name": "Acre",
            "capital": "Rio Branco",
        },
        {
            "acronym": "AL",
            "name": "Alagoas",
            "capital": "Maceió",
        },
        {
            "acronym": "AP",
            "name": "Amapá",
            "capital": "Macapá",
        },
        {
            "acronym": "AM",
            "name": "Amazonas",
            "capital": "Manaus",
        },
        {
            "acronym": "BA",
            "name": "Bahia",
            "capital": "Salvador",
        },
        {
            "acronym": "CE",
            "name": "Ceará",
            "capital": "Fortaleza",
        },
        {
            "acronym": "DF",
            "name": "Distrito Federal",
            "capital": "Brasília",
        },
        {
            "acronym": "ES",
            "name": "Espírito Santo",
            "capital": "Vitória",
        },
        {
            "acronym": "GO",
            "name": "Goiás",
            "capital": "Goiânia",
        },
        {
            "acronym": "MA",
            "name": "Maranhão",
            "capital": "São Luís",
        },
        {
            "acronym": "MT",
            "name": "Mato Grosso",
            "capital": "Cuiabá",
        },
        {
            "acronym": "MS",
            "name": "Mato Grosso do Sul",
            "capital": "Campo Grande",
        },
        {
            "acronym": "MG",
            "name": "Minas Gerais",
            "capital": "Belo Horizonte",
        },
        {
            "acronym": "PA",
            "name": "Pará",
            "capital": "Belém",
        },
        {
            "acronym": "PB",
            "name": "Paraíba",
            "capital": "João Pessoa",
        },
        {
            "acronym": "PR",
            "name": "Paraná",
            "capital": "Londrina",  # Ajustado conforme cliente solicitou
        },
        {
            "acronym": "PE",
            "name": "Pernambuco",
            "capital": "Recife",
        },
        {
            "acronym": "PI",
            "name": "Piauí",
            "capital": "Teresina",
        },
        {
            "acronym": "RJ",
            "name": "Rio de Janeiro",
            "capital": "Rio de Janeiro",
        },
        {
            "acronym": "RN",
            "name": "Rio Grande do Norte",
            "capital": "Natal",
        },
        {
            "acronym": "RS",
            "name": "Rio Grande do Sul",
            "capital": "Porto Alegre",
        },
        {
            "acronym": "RO",
            "name": "Rondônia",
            "capital": "Porto Velho",
        },
        {
            "acronym": "RR",
            "name": "Roraima",
            "capital": "Boa Vista",
        },
        {
            "acronym": "SC",
            "name": "Santa Catarina",
            "capital": "Florianópolis",
        },
        {
            "acronym": "SP",
            "name": "São Paulo",
            "capital": "São Paulo",
        },
        {
            "acronym": "SE",
            "name": "Sergipe",
            "capital": "Aracaju",
        },
        {
            "acronym": "TO",
            "name": "Tocantins",
            "capital": "Palmas",
        },
    ]

    def __init__(self):
        super().__init__()
        self.df_states = pd.DataFrame(self.STATES)
        self.data_frames = {}

    def split_state_city(self, uf):
        """Procura o estado pelo nome no DataFrame de estados"""
        original_uf = uf
        splited_data = self.split_strings(uf)

        if len(splited_data) == 2:
            if len(splited_data[0]) == 2:
                uf = splited_data[0]
            else:
                uf = splited_data[0]
        elif len(splited_data) > 2:
            if re.search("-", original_uf):
                uf = original_uf.split("-")[0].strip()
            else:
                uf = " ".join(splited_data)

        states = self.df_states[
            self.df_states.apply(
                lambda row: uf in row["acronym"] or uf in row["name"], axis=1
            )
        ]

        if not states.empty:
            state_acronym = states.iloc[0]["acronym"]
            city = original_uf.replace(states.iloc[0]["acronym"], "").strip()
        else:
            city, state_acronym = original_uf, ""

        if not city:
            city = states.iloc[0]["capital"]

        return city.upper(), state_acronym.upper()

    def extract_table_data(self, table_identifiers, headers):
        """Extrai dados de uma tabela específica do BeautifulSoup e retorna um DataFrame"""
        table = self.soup.find("table", table_identifiers)
        if not table:
            print(f"Table with {table_identifiers} not found.")
            return pd.DataFrame()

        rows = []
        for tr in table.find_all("tr", {"class": "conteudo"}):
            cells = tr.find_all("td")
            if cells[0].text.strip() in headers:
                continue
            row = [cells[i].text.strip() for i in range(len(headers))]
            rows.append(row)

        return pd.DataFrame(rows, columns=headers)

    def customize_df(self, data):
        def join_dataframe(data_dict):
            for item in data_dict:
                if not item.get("join"):
                    continue
                item["df"].insert(0, "Tipo", item.get("title"))

            return pd.concat(
                [i.get("df") for i in data_dict if i.get("join", False)],
                ignore_index=True,
            )

        def pivoting_year(df, column_value):
            current_year = datetime.now().year
            last_year = datetime.now().year - 1

            df_current_year = df[[column_value, "Valor"]].copy()
            df_current_year['Ano'] = current_year

            df_last_year = df[[column_value, f"Valor {last_year}"]].copy()
            df_last_year['Ano'] = last_year
            df_last_year.rename(columns={f"Valor {last_year}": 'Valor'}, inplace=True)

            df_result = pd.concat([df_current_year, df_last_year], ignore_index=True)
            df_result = df_result[['Ano', column_value, 'Valor']]

            return df_result

        yesterday = datetime.now() - timedelta(days=1)
        current_date = self.get_previous_weekday(yesterday)
        year, month, _ = current_date.strftime("%Y-%m-%d").split("-")
        date = current_date.strftime("%d/%m/%Y")

        df_result = join_dataframe(data)
        df_result[["Cidade", "Estado"]] = df_result["UF"].apply(
            lambda x: pd.Series(self.split_state_city(x))
        )
        df_result = df_result.assign(Data=date, Ano=year, Mês=month)

        df_result = df_result[["Tipo", "Cidade", "Estado", "Valor"]]

        # Condicionais de validação da planilha
        df_result.loc[
            (df_result["Tipo"] == "BOI GORDO - CHINA"), ["Cidade", "Tipo"]
        ] = ["CHINA", "BOI GORDO"]
        df_result.loc[(df_result["Tipo"] == "CEPEA"), ["Tipo", "Cidade", "Estado"]] = [
            "BOI GORDO",
            "CHINA",
            "CEPEA",
        ]

        self.save_dataframe_to_csv(df_result, "resumo.csv")

        df_boi_no_mundo = list(
            filter(lambda x: x.get("title", "") == "BOI NO MUNDO", data)
        )[0].get("df")
        df_atacado = list(filter(lambda x: x.get("title", "") == "ATACADO", data))[
            0
        ].get("df")

        df_boi_no_mundo = pivoting_year(df=df_boi_no_mundo, column_value="Pais")
        df_atacado = pivoting_year(df=df_atacado, column_value="Atacado SP")

        self.data_frames = {
            'Resumo': df_result,
            'Boi no mundo': df_boi_no_mundo,
            'Atacado': df_atacado,
        }

        return self.data_frames


    def execute(self):
        """Raspa cotações das páginas especificadas e envia por email"""
        extracts = [
            {
                "link": "boi-gordo",
                "join": True,
                "params": [
                    {
                        "title": "BOI GORDO - CHINA",
                        "table_identifiers": {
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660px",
                        },
                        "headers": ["UF", "Valor"],
                        "save_to": "boi_china_prazo.csv",
                    },
                    {
                        "title": "BOI GORDO",
                        "table_identifiers": {
                            "border": "0",
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660",
                        },
                        "headers": ["UF", "Valor"],
                        "save_to": "boi_mercado_fisico.csv",
                    },
                ],
            },
            {
                "link": "vaca-gorda",
                "join": True,
                "params": [
                    {
                        "title": "VACA GORDA",
                        "table_identifiers": {
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660px",
                        },
                        "headers": ["UF", "Valor"],
                        "save_to": "vaca_mercado_fisico.csv",
                    }
                ],
            },
            {
                "link": "novilha",
                "join": True,
                "params": [
                    {
                        "title": "NOVILHA",
                        "table_identifiers": {
                            "border": "0",
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660",
                            "style": "margin-top: 10px",
                        },
                        "headers": ["UF", "Valor"],
                        "save_to": "novilha_mercado_fisico.csv",
                    }
                ],
            },
            {
                "link": "boi-no-mundo",
                "join": False,
                "params": [
                    {
                        "title": "BOI NO MUNDO",
                        "table_identifiers": {
                            "border": "0",
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660",
                        },
                        "headers": ["Pais", "Valor", f"Valor {datetime.now().year -1}"],
                        "save_to": "boi_no_mundo.csv",
                    }
                ],
            },
            {
                "link": "atacado",
                "join": False,
                "params": [
                    {
                        "title": "ATACADO",
                        "table_identifiers": {
                            "border": "0",
                            "cellpadding": "0",
                            "cellspacing": "0",
                            "width": "660",
                        },
                        "headers": [
                            "Atacado SP",
                            "Valor",
                            "Img",
                            f"Valor {datetime.now().year -1}",
                        ],
                        "save_to": "atacado.csv",
                        "column_remove": ["Img"],
                    }
                ],
            },
        ]

        scrapes = []
        # Scot Consultoria
        for extract in extracts:
            url = f"https://www.scotconsultoria.com.br/cotacoes/{extract['link']}/?ref=smn"
            self.fetch_page_content(url)

            if self.soup:
                for params in extract["params"]:
                    table_identifiers = params.get("table_identifiers")
                    headers = params.get("headers")
                    save_to = params.get("save_to")
                    df = self.extract_table_data(table_identifiers, headers)
                    columns_remove = params.get("column_remove", [])
                    if len(columns_remove) > 0:
                        for col in columns_remove:
                            df = df.drop(columns=[col])

                    file = self.save_dataframe_to_csv(df, save_to)
                    scrapes.append(
                        {
                            "title": params.get("title"),
                            "df": df,
                            "file": file,
                            "join": extract.get("join", False),
                        }
                    )

            else:
                print(f"Page not found {url}")

        # CEPEA
        url = "https://www.cepea.esalq.usp.br/br/indicador/boi-gordo.aspx"
        self.fetch_page_content(url)

        if self.soup:
            table = self.soup.find("table", {"id": "imagenet-indicador1"})
            if table:
                first_row = table.find("tbody").find("tr")
                date = first_row.find_all("td")[0].text.strip()
                value = first_row.find_all("td")[1].text.strip()

                df = pd.DataFrame([[date, value]], columns=["UF", "Valor"])
                file = self.save_dataframe_to_csv(df, "cpea.csv")
                scrapes.append({"title": "CEPEA", "df": df, "file": file, "join": True})
            else:
                print("Table not found")

        return self.customize_df(scrapes)





