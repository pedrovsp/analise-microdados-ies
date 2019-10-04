import pandas as pd
from sqlalchemy import create_engine 
import config
import tb_aluno_column_names

# TODO Read from input

# CURSO
# filePath = '../microdados_censo_superior_2016/DADOS/DM_CURSO.CSV'
# validColumns = [ 0, 2, 4, 6, 10, 11, 13, 14, 15, 17, *range(19, 22), 25, 27 ]
# tableName = "tb_curso"
# indexLabel = "CO_CURSO"

# IES
# filePath = '../microdados_censo_superior_2016/DADOS/DM_IES.CSV'
# validColumns = [ 0, 1, 2, 5, 6, *range(9, 13) ]
# tableName = "tb_ies"
# indexLabel = "CO_ALUNO_CURSO"

# ALUNO
filePath = '../microdados_censo_superior_2016/DADOS/DM_ALUNO.CSV'
validColumns = [ 0, 2, 6, 9, 11, 13, 23, 25, 28, 30, 35, 39, 55, *range(57, 75), 105, 107 ]
tableName = "tb_aluno"
indexLabel = "CO_ALUNO_CURSO"
columnsToDrop = [ 'IN_ING_VESTIBULAR',
       'IN_ING_ENEM', 'IN_ING_AVALIACAO_SERIADA',
       'IN_ING_SELECAO_SIMPLIFICADA', 'IN_ING_SELECAO_VAGA_REMANESC',
       'IN_ING_SELECAO_VAGA_PROG_ESPEC', 'IN_ING_TRANSF_EXOFFICIO',
       'IN_ING_DECISAO_JUDICIAL', 'IN_ING_CONVENIO_PECG',
       'IN_RESERVA_ETNICO', 'IN_RESERVA_DEFICIENCIA',
       'IN_RESERVA_ENSINO_PUBLICO', 'IN_RESERVA_RENDA_FAMILIAR',
       'IN_RESERVA_OUTRA' ]

encodingVar = "cp1252"
batchSize = 1000000

engine = None
df = None
columnNames = []

def ReadRows(i, columns):
    skip = i * batchSize + 1

    dataFrame = pd.read_csv(filePath, sep='|', header = None, encoding=encodingVar, skiprows=skip, nrows=batchSize, usecols=validColumns, names=columns)
    dataFrame['CO_INGRESSO'] = ''
    dataFrame['CO_RESERVA'] = ''
    dfFiltered = dataFrame[(dataFrame.CO_OCDE_AREA_DETALHADA > 480) & (dataFrame.CO_OCDE_AREA_DETALHADA < 484) & (dataFrame.CO_MODALIDADE_ENSINO == 1)]
    for index, row in dfFiltered.iterrows():
        dfFiltered.CO_INGRESSO[index] = ParseCOIngresso(row)
        dfFiltered.CO_RESERVA[index] = ParseCOReserva(row)
    dfFiltered = dfFiltered.drop(columns=columnsToDrop)
    return dfFiltered

def CreateTable(engine):
    df = pd.read_csv(filePath, sep='|', header = 0, encoding=encodingVar, nrows=0, usecols=validColumns)
    df['CO_INGRESSO'] = ''
    df['CO_RESERVA'] = ''
    df = df.drop(columns=columnsToDrop)
    df.to_sql(name=tableName, con=engine, if_exists='replace', index=False, index_label=indexLabel)
    return df.columns

def CreateConnection():
    engine=create_engine("mysql+pymysql://" + config.username + ":" + config.password + "@localhost/microdados2016")
    return engine

def ParseCOIngresso(row):
    if (row.IN_ING_VESTIBULAR == 1): return 'V'
    if (row.IN_ING_ENEM == 1): return 'E'
    if (row.IN_ING_AVALIACAO_SERIADA == 1): return 'AS'
    if (row.IN_ING_SELECAO_SIMPLIFICADA == 1): return 'SS'
    if (row.IN_ING_SELECAO_VAGA_REMANESC == 1): return 'VR'
    if (row.IN_ING_SELECAO_VAGA_PROG_ESPEC == 1): return 'VP'
    if (row.IN_ING_TRANSF_EXOFFICIO == 1): return 'TE'
    if (row.IN_ING_DECISAO_JUDICIAL == 1): return 'DJ'
    if (row.IN_ING_CONVENIO_PECG == 1): return 'CP'
    return 'NA'

def ParseCOReserva(row):
    if (row.IN_RESERVA_VAGAS == 1):
        if (row.IN_RESERVA_ETNICO == 1): return 'RE'
        if (row.IN_RESERVA_DEFICIENCIA == 1): return 'RD'
        if (row.IN_RESERVA_ENSINO_PUBLICO == 1): return 'EP'
        if (row.IN_RESERVA_RENDA_FAMILIAR == 1): return 'RF'
        if (row.IN_RESERVA_OUTRA == 1): return 'RO'
        return 'NA'
    else:
        return 'NA'

engine = CreateConnection()
# Enable to create table
columnNames = CreateTable(engine)

for i in range(0,12):
    df = ReadRows(i, tb_aluno_column_names.columnNames)
    df.to_sql(name=tableName, con=engine, if_exists='append', index=False, index_label=indexLabel)
    print("inserted ", (i + 1) * batchSize)

df = pd.read_sql_table(tableName, engine)
df.to_csv(path_or_buf = 'tb_aluno.csv', sep = "|", encoding = encodingVar)