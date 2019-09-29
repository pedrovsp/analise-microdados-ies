import pandas as pd
from sqlalchemy import create_engine 
import config

encodingVar = "cp1252"
filePath = '../microdados_censo_superior_2016/DADOS/DM_ALUNO.CSV'
validColumns = [ 0, 2, 6, 9, 11, 13, 28, 30, 35, 39, 55, *range(57, 75), 105, 107 ]
batchSize = 100000
engine = None
df = None

def ReadRows(i):
	dataFrame = pd.read_csv(filePath, sep='|', header = None, encoding=encodingVar, skiprows=(i*batchSize), nrows=batchSize, usecols=validColumns)

	return dataFrame

def ReadHeaders():
	headers = pd.read_csv(filePath, sep='|', header = 0, encoding=encodingVar, nrows=1, usecols=validColumns)
	return headers

def CreateConnection():
	engine=create_engine("mysql+pymysql://" + config.username + ":" + config.password + "@localhost/microdados2016")
	return engine

engine = CreateConnection()
df = ReadHeaders()
df.to_sql(name='aluno', con=engine, if_exists='append')