import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
import os

class VendaProcessor:
    def __init__(self, data_inicial, data_final, paths_cielo, paths_vendas):
        self.data_inicial = data_inicial
        self.data_final = data_final
        self.paths_cielo = paths_cielo
        self.paths_vendas = paths_vendas
        self.coluna_b = None
        self.error = None

        try:
            if self.paths_cielo:
                df_cielo_temp = pd.read_excel(self.paths_cielo[0], skiprows=9, usecols="I", engine="openpyxl")
                if not df_cielo_temp.empty:
                    estabelecimento = df_cielo_temp.iloc[0, 0]
                    # 5112 - Loja SELS
                    if str(estabelecimento) == "1049143393":
                        self.coluna_b = "1"
                    elif str(estabelecimento) == "2889751230":
                        self.coluna_b = "6"
                    # 5124 - FAAMA 1109206094
                    elif str(estabelecimento) == "1030032510":
                        self.coluna_b = "5"
                    elif str(estabelecimento) == "1109206094":
                        self.coluna_b = "6"
                    elif str(estabelecimento) == "2809433369":
                        self.coluna_b = "1"
                    else:
                        self.error = "Código de estabelecimento não reconhecido."
                else:
                    self.error = "O primeiro arquivo Cielo está vazio ou o formato está incorreto."
            else:
                self.error = "Nenhum arquivo Cielo selecionado."
        except Exception as e:
            self.error = f"Erro ao identificar o estabelecimento: {e}"

    def _converter_valor(self, valor):
        return int(Decimal(str(valor)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100)

    def processar(self):
        if self.error:
            return None, self.error

        df_cielo_consolidado = pd.DataFrame()
        df_vendas_consolidado = pd.DataFrame()
        
        try:
            for path in self.paths_cielo:
                df_temp = pd.read_excel(path, skiprows=9, usecols="A:I", engine="openpyxl")
                df_temp.columns = ["Data de pagamento", "Data do lançamento", "NSU/DOC", "Valor bruto", "Valor líquido", 
                                "Data prevista de pagamento", "Número da parcela", "Quantidade total de parcelas", "Estabelecimento"]
                df_temp["Data do lançamento"] = pd.to_datetime(df_temp["Data do lançamento"], errors='coerce', dayfirst=True)
                df_temp["Data prevista de pagamento"] = pd.to_datetime(df_temp["Data prevista de pagamento"], errors='coerce', dayfirst=True)
                df_cielo_consolidado = pd.concat([df_cielo_consolidado, df_temp], ignore_index=True)
            
            for path in self.paths_vendas:
                df_temp = pd.read_excel(path, skiprows=9, usecols="A:E", engine="openpyxl")
                df_temp.columns = ["Data da venda", "NSU/DOC", "Valor bruto", "Número da máquina", "Estabelecimento"]
                df_temp["Data da venda"] = pd.to_datetime(df_temp["Data da venda"], errors='coerce', dayfirst=True)
                df_vendas_consolidado = pd.concat([df_vendas_consolidado, df_temp], ignore_index=True)
        
        except Exception as e:
            return None, f"Erro ao ler arquivos: {e}"
        
        df_cielo = df_cielo_consolidado[
            (df_cielo_consolidado["Data do lançamento"] >= self.data_inicial) & 
            (df_cielo_consolidado["Data do lançamento"] <= self.data_final)
        ]

        df_vendas = df_vendas_consolidado[
            (df_vendas_consolidado["Data da venda"] >= self.data_inicial) & 
            (df_vendas_consolidado["Data da venda"] <= self.data_final)
        ]
        
        datas_para_agrupar = pd.Series(pd.concat([
            df_cielo['Data do lançamento'].dt.normalize().dropna(),
            df_vendas['Data da venda'].dt.normalize().dropna()
        ]).unique()).sort_values()

        if datas_para_agrupar.empty:
            return None, "Nenhuma venda encontrada no intervalo informado."

        registros_diarios = []
        for data in datas_para_agrupar:
            df_cielo_data = df_cielo[df_cielo["Data do lançamento"].dt.normalize() == data].copy()
            df_vendas_data = df_vendas[df_vendas["Data da venda"].dt.normalize() == data].copy()
            
            if df_cielo_data.empty and df_vendas_data.empty:
                continue

            df = df_cielo_data.copy()
            df = df[~(df["NSU/DOC"].isna() & (df["Valor líquido"] < 0))]
            df["NSU/DOC"] = df["NSU/DOC"].astype("Int64").astype(str)
            df["Número da parcela"] = df["Número da parcela"].fillna(1).astype(int).astype(str)
            df["Quantidade total de parcelas"] = df["Quantidade total de parcelas"].fillna(1).astype(int).astype(str)
            df["Concatenado"] = df.apply(lambda row: f"Doc.{row['NSU/DOC']} - {row['Data prevista de pagamento'].strftime('%d/%m/%Y')} - {row['Número da parcela']}/{row['Quantidade total de parcelas']}", axis=1)

            df_resultado = pd.DataFrame({
                "Coluna A": "1139008",
                "Coluna B": self.coluna_b,
                "Coluna C": "10",
                "Coluna D": "101",
                "Coluna E": "0A",
                "Coluna F": df["Valor líquido"].fillna(0).apply(self._converter_valor).astype(int),
                "Coluna G": "N",
                "Coluna H": df["Concatenado"]
            })

            df_comissao = pd.DataFrame({
                "Coluna A": "4121013",
                "Coluna B": "",
                "Coluna C": "10",
                "Coluna D": "101",
                "Coluna E": "0E",
                "Coluna F": (df["Valor bruto"].fillna(0) - df["Valor líquido"].fillna(0)).apply(self._converter_valor).astype(int),
                "Coluna G": "N",
                "Coluna H": df.apply(lambda row: f"Comissão Cartão Cielo - {row['Data do lançamento'].strftime('%d/%m/%Y')}", axis=1)
            })
            df_comissao = df_comissao[df_comissao["Coluna F"] != 0]

            df_vendas_resultado = None
            if not df_vendas_data.empty:
                df_vendas_resultado = pd.DataFrame({
                    "Coluna A": "2139090",
                    "Coluna B": "4",
                    "Coluna C": "10",
                    "Coluna D": "101",
                    "Coluna E": "0A",
                    "Coluna F": df_vendas_data["Valor bruto"].fillna(0).apply(self._converter_valor).astype(int) * -1,
                    "Coluna G": "N",
                    "Coluna H": df_vendas_data.apply(lambda row: f"Doc.{row['NSU/DOC']} - {row['Data da venda'].strftime('%d/%m/%Y')} - POS:{row['Número da máquina']}", axis=1)
                })

            if df_vendas_resultado is not None:
                df_dia_completo = pd.concat([df_resultado, df_comissao, df_vendas_resultado], ignore_index=True)
            else:
                df_dia_completo = pd.concat([df_resultado, df_comissao], ignore_index=True)

            registros_diarios.append((data.strftime("%Y-%m-%d"), df_dia_completo))

        return registros_diarios, None