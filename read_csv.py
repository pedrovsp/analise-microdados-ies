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

encodingVar = "cp1252"
batchSize = 1000000

engine = None
df = None
columnNames = []

def ReadRows(i, columns):
    skip = i * batchSize + 1

    dataFrame = pd.read_csv(filePath, sep='|', header = None, encoding=encodingVar, skiprows=skip, nrows=batchSize, usecols=validColumns, names=columns)
    # dfFiltered = dataFrame[(dataFrame.CO_OCDE_AREA_GERAL == 4) & (dataFrame.CO_OCDE_AREA_ESPECIFICA > 80) & (dataFrame.CO_OCDE_AREA_ESPECIFICA < 83) & (dataFrame.CO_MODALIDADE_ENSINO == 1)]
    dfFiltered = dataFrame[(dataFrame.CO_OCDE_AREA_DETALHADA > 480) & (dataFrame.CO_OCDE_AREA_DETALHADA < 484) & (dataFrame.CO_MODALIDADE_ENSINO == 1)]
    return dfFiltered

def CreateTable(engine):
    df = pd.read_csv(filePath, sep='|', header = 0, encoding=encodingVar, nrows=0, usecols=validColumns)
    df.to_sql(name=tableName, con=engine, if_exists='replace', index=False, index_label=indexLabel)
    return df.columns

def CreateConnection():
    engine=create_engine("mysql+pymysql://" + config.username + ":" + config.password + "@localhost/microdados2016")
    return engine

engine = CreateConnection()
# Enable to create table
columnNames = CreateTable(engine)

for i in range(0,14):
    df = ReadRows(i, tb_aluno_column_names.columnNames)
    df.to_sql(name=tableName, con=engine, if_exists='append', index=False, index_label=indexLabel)
    print("inserted ", (i + 1) * batchSize)
