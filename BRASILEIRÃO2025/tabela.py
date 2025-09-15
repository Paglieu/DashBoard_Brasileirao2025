from bs4 import BeautifulSoup
import requests, pandas as pd, pyodbc
from io import StringIO

link = 'https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/2025'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36"
}

session = requests.Session()
session.headers.update(headers)
requisicao = session.get(link)
print(requisicao)

site = BeautifulSoup(requisicao.content, 'html.parser')
tabela = site.find(name='table')
tabela_str = str(tabela)

df = pd.read_html(StringIO(tabela_str))[0]
df.rename(columns={'Classificação': 'Time','PTSPontos': 'PTS', 'JJogos': 'J', 'VVitórias': 'V', 'EEmpates': 'E', 'DDerrotas': 'D', 'GPGols Prós': 'GP', 'GCGols Contras': 'GC','SGSaldos de Gols': 'SG', 'CACartões Amarelos': 'CA', 'CVCartões Vermelhos': 'CV', '%Aproveitamento': '%' }, inplace=True)
df.drop(columns={'Recentes', 'PróxPróximo'}, inplace=True)
df['Time'] = df['Time'].str.replace(r'^\s*\d+\s*|[^A-Za-zÀ-ÿ\s]', '', regex=True)
df.insert(0, 'Classificação', range(1, len(df) + 1))

print(df)

dados_conexao = (
    "Driver={SQL Server};" 
    "Server=DESKTOP-Q6OETKU\SQLPUCMINAS;"
    "DataBase=Brasileirao2025;"
    "Trusted_Connection=yes;"
)

conexao = pyodbc.connect(dados_conexao)
cursor = conexao.cursor()
print("Conexão com o Banco de Dados bem sucedida")

cursor.execute("TRUNCATE TABLE tabela;")
conexao.commit()

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO tabela
        ([Classificação], [Time], [PTS], [J], [V], [E], [D], [GP], [GC], [SG], [CA], [CV], [%])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(row))

conexao.commit()
cursor.close()
conexao.close()

print("Tabela atualizada com sucesso!")