import pandas as pd
from sqlalchemy import create_engine 
import config
import tb_aluno_column_names_pred

# ALUNO
filePath = '../microdados_censo_superior_2016/DADOS/DM_ALUNO.CSV'
validColumns = [ 2, 9, 11, 13, 23, 28, 30, 35, 55, 57, *range(59, 75), 105 ]
tableName = "tb_aluno_predictive"
columnsToDrop = [ 'IN_ING_VESTIBULAR',
       'IN_ING_ENEM', 'IN_ING_AVALIACAO_SERIADA',
       'IN_ING_SELECAO_SIMPLIFICADA', 'IN_ING_SELECAO_VAGA_REMANESC',
       'IN_ING_SELECAO_VAGA_PROG_ESPEC', 'IN_ING_TRANSF_EXOFFICIO',
       'IN_ING_DECISAO_JUDICIAL', 'IN_ING_CONVENIO_PECG',
       'IN_RESERVA_ETNICO', 'IN_RESERVA_DEFICIENCIA',
       'IN_RESERVA_ENSINO_PUBLICO', 'IN_RESERVA_RENDA_FAMILIAR',
       'IN_RESERVA_OUTRA', 'CO_MODALIDADE_ENSINO', 'IN_RESERVA_VAGAS' ]

encodingVar = "cp1252"
batchSize = 1000000

engine = None
df = None
columnNames = []

def CreateConnection():
    engine=create_engine("mysql+pymysql://" + config.username + ":" + config.password + "@localhost/microdados2016")
    return engine

def CreateTable(engine):
    df = pd.read_csv(filePath, sep='|', header = 0, encoding=encodingVar, nrows=0, usecols=validColumns)
    df['CO_INGRESSO'] = ''
    df['CO_RESERVA'] = ''
    df = df.drop(columns=columnsToDrop)
    df.to_sql(name=tableName, con=engine, if_exists='replace', index=False)
    return df.columns

def ReadRows(i, columns):
    skip = i * batchSize + 1

    dataFrame = pd.read_csv(filePath, sep='|', header = None, encoding=encodingVar, skiprows=skip, nrows=batchSize, usecols=validColumns, names=columns)
    dataFrame['CO_INGRESSO'] = ''
    dataFrame['CO_RESERVA'] = ''
    dfFiltered = dataFrame[ (dataFrame.CO_OCDE_AREA_DETALHADA > 480) & 
                            (dataFrame.CO_OCDE_AREA_DETALHADA < 484) & 
                            (dataFrame.CO_MODALIDADE_ENSINO == 1) & 
                            (dataFrame.CO_ALUNO_SITUACAO > 2) & 
                            (dataFrame.CO_ALUNO_SITUACAO < 7) ]

    # Functions to parse data to
    for index, row in dfFiltered.iterrows():
        dfFiltered.CO_INGRESSO[index] = ParseCOIngresso(row)
        dfFiltered.CO_RESERVA[index] = ParseCOReserva(row)
        dfFiltered.CO_ALUNO_SITUACAO[index] = ParseCOAlunoSituacao(row)
        dfFiltered.DT_INGRESSO_CURSO[index] = ParseDTIngressoCurso(row)
        dfFiltered.NU_IDADE_ALUNO[index] = ParseNUIdadeAluno(row)
        dfFiltered.CO_CATEGORIA_ADMINISTRATIVA[index] = ParseCOCategoriaAdm(row)
        dfFiltered.CO_TURNO_ALUNO[index] = ParseCOTurnoAluno(row)
        dfFiltered.CO_COR_RACA_ALUNO[index] = ParseCORaca(row)
        dfFiltered.IN_SEXO_ALUNO[index] = ParseINSexo(row)
        dfFiltered.CO_GRAU_ACADEMICO[index] = ParseCOGrauAcademico(row)
        dfFiltered.CO_TIPO_ESCOLA_ENS_MEDIO[index] = ParseCOTipoEscola(row)
        # dfFiltered.CO_OCDE_AREA_DETALHADA[index] = ParseCOAreaOCDE(row)
        dfFiltered.QT_CARGA_HORARIA_TOTAL[index] = ParseNUCargaHoraria(row)

    dfFiltered = dfFiltered.drop(columns=columnsToDrop)
    return dfFiltered

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

def ParseCOAlunoSituacao(row):
    if (row.CO_ALUNO_SITUACAO == 4 or row.CO_ALUNO_SITUACAO == 5): return 'E'
    if (row.CO_ALUNO_SITUACAO == 3): return 'T'
    if (row.CO_ALUNO_SITUACAO == 6): return 'F'
    return 'NA'

def ParseDTIngressoCurso(row):
    return row.DT_INGRESSO_CURSO.split('/')[2]


def ParseNUIdadeAluno(row):
    if (row.NU_IDADE_ALUNO < 18): return 'AE'
    if (row.NU_IDADE_ALUNO >= 18 and row.NU_IDADE_ALUNO < 30): return 'J'
    if (row.NU_IDADE_ALUNO >= 30 and row.NU_IDADE_ALUNO < 60): return 'AO'
    if (row.NU_IDADE_ALUNO >= 60): return 'I'
    return 'NA'

def ParseCOCategoriaAdm(row):
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 1): return 'PF'
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 2): return 'PE'
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 3): return 'PM'
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 4): return 'PCL'
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 5): return 'PSL'
    if (row.CO_CATEGORIA_ADMINISTRATIVA == 7): return 'E'
    return 'NA'

def ParseCOTurnoAluno(row):
    if (row.CO_TURNO_ALUNO == 1): return 'M'
    if (row.CO_TURNO_ALUNO == 2): return 'V'
    if (row.CO_TURNO_ALUNO == 3): return 'N'
    if (row.CO_TURNO_ALUNO == 4): return 'I'
    if (row.CO_TURNO_ALUNO == '.'): return '.'
    return 'NA'    

def ParseCORaca(row):
    if (row.CO_COR_RACA_ALUNO == 1): return 'B'
    if (row.CO_COR_RACA_ALUNO == 2): return 'PR'
    if (row.CO_COR_RACA_ALUNO == 3): return 'PA'
    if (row.CO_COR_RACA_ALUNO == 4): return 'A'
    if (row.CO_COR_RACA_ALUNO == 5): return 'I'
    return 'NA'

def ParseINSexo(row):
    if (row.IN_SEXO_ALUNO == 0): return 'M'
    if (row.IN_SEXO_ALUNO == 1): return 'F'
    return 'NA'    

def ParseCOGrauAcademico(row):
    if (row.CO_GRAU_ACADEMICO == 1): return 'B'
    if (row.CO_GRAU_ACADEMICO == 2): return 'L'
    if (row.CO_GRAU_ACADEMICO == 3): return 'T'
    return 'NA'

def ParseCOTipoEscola(row):
    if (row.CO_TIPO_ESCOLA_ENS_MEDIO == 0): return 'PV'
    if (row.CO_TIPO_ESCOLA_ENS_MEDIO == 1): return 'PU'
    return 'NA'

def ParseCOAreaOCDE(row):
    if (row.CO_OCDE_AREA_DETALHADA == 481): return 'CC'
    if (row.CO_OCDE_AREA_DETALHADA == 482): return 'UC'
    if (row.CO_OCDE_AREA_DETALHADA == 483): return 'PI'
    return 'NA'

def ParseNUCargaHoraria(row):
    if (row.QT_CARGA_HORARIA_TOTAL >= 800 and row.QT_CARGA_HORARIA_TOTAL < 2000): return 2
    if (row.QT_CARGA_HORARIA_TOTAL >= 2000 and row.QT_CARGA_HORARIA_TOTAL < 3000): return 3
    if (row.QT_CARGA_HORARIA_TOTAL >= 3000 and row.QT_CARGA_HORARIA_TOTAL < 4000): return 4
    if (row.QT_CARGA_HORARIA_TOTAL >= 4000 and row.QT_CARGA_HORARIA_TOTAL < 5000): return 5
    if (row.QT_CARGA_HORARIA_TOTAL >= 5000 and row.QT_CARGA_HORARIA_TOTAL < 10000): return 10
    if (row.QT_CARGA_HORARIA_TOTAL >= 10000): return 11
    return 'NA'

engine = CreateConnection()
# Enable to create table
columnNames = CreateTable(engine)

for i in range(0,12):
    df = ReadRows(i, tb_aluno_column_names_pred.columnNames)
    df.to_sql(name=tableName, con=engine, if_exists='append', index=False)
    print("inserted ", (i + 1) * batchSize)

# Generate CSV
df = pd.read_sql_table(tableName, engine)
df.to_csv(path_or_buf = 'tb_aluno_pred.csv', sep = ",", encoding = encodingVar)