import pandas as pd
from sqlalchemy import create_engine 
import config

encodingVar = "cp1252"
filePath = '../microdados_censo_superior_2016/DADOS/DM_ALUNO.CSV'
validColumns = [ 0, 2, 6, 9, 11, 13, 25, 28, 30, 35, 39, 55, *range(57, 75), 105, 107 ]
batchSize = 100000

engine = None
df = None
columnNames = []

def ReadRows(i):
    skip = i * batchSize + 1

    dataFrame = pd.read_csv(filePath, sep='|', header = None, encoding=encodingVar, skiprows=skip, nrows=batchSize, usecols=validColumns, names=columnNames)

    return dataFrame

def CreateTable(engine):
    df = pd.read_csv(filePath, sep='|', header = 0, encoding=encodingVar, nrows=0, usecols=validColumns)
    df.to_sql(name='aluno', con=engine, if_exists='replace', index=False, index_label="CO_ALUNO_CURSO")
    return df.columns

def CreateConnection():
    engine=create_engine("mysql+pymysql://" + config.username + ":" + config.password + "@localhost/microdados2016")
    return engine

engine = CreateConnection()
columnNames = CreateTable(engine)

for i in range(1):
    df = ReadRows(i)
    df.to_sql(name='aluno', con=engine, if_exists='append', index=False, index_label="CO_ALUNO_CURSO")
    print("inserted ", i + 1 * batchSize)