# Projeto - Prevendo Doencas Cardiacas

# Etapa 1 - Carregando o dataset no Hive e visualizando os dados com SQL

CREATE DATABASE usecase location '/user/cloudera/projeto'; 

CREATE TABLE pacientes (ID INT, IDADE INT, SEXO INT, PRESSAO_SANGUINEA INT, COLESTEROL INT, ACUCAR_SANGUE INT, ECG INT, BATIMENTOS INT, DOENCA INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH 'pacientes.csv' OVERWRITE INTO TABLE pacientes;

SELECT count(*) FROM pacientes;

SELECT doenca, count(*), avg(idade), avg(pressao_sanguinea), avg(colesterol), avg(acucar_sangue), avg(batimentos) FROM pacientes GROUP BY doenca;



# Etapa 2 - Analise Exploratoria e pre-processamento nos dados com Pig

dadosPacientes = LOAD 'pacientes.csv' USING PigStorage(',') AS ( ID:int, Idade:int, Sexo:int, PressaoSanguinea:int, Colesterol:int, AcucarSangue:int, ECG:int, Batimentos:int, Doenca:int);

REGISTER datafu-1.2.0.jar; 

DEFINE Quantile datafu.pig.stats.Quantile('0.0','0.25','0.5','0.75','1.0'); 

diseaseGroup = GROUP dadosPacientes BY Doenca; 

quanData = FOREACH diseaseGroup GENERATE group, Quantile(dadosPacientes.Idade) as Age, Quantile(dadosPacientes.PressaoSanguinea) as BP, Quantile(dadosPacientes.Colesterol) as Colesterol, Quantile(dadosPacientes.AcucarSangue) as AcucarSangue; 

DUMP quanData;


# Etapa 3 - Transformação de Dados com o Pig

ageRange = FOREACH dadosPacientes GENERATE ID, CEIL(Age/10) as AgeRange; 
bpRange = FOREACH dadosPacientes GENERATE ID, CEIL(BloodPressure/25) as bpRange; 
chRange = FOREACH dadosPacientes GENERATE ID, CEIL(Cholesterol/25) as chRange; 
hrRange = FOREACH dadosPacientes GENERATE ID, CEIL(MaxHeartRate/25) as hrRange; 
enhancedData = JOIN dadosPacientes by ID, ageRange by ID, bpRange by ID, hrRange by ID; 
describe enhancedData;

predictionData = FOREACH enhancedData GENERATE dadosPacientes::Sexo, dadosPacientes::AcucarSangue, patientData::ECG, ageRange::AgeRange, bpRange::bpRange, hrRange::hrRange, dadosPacientes::Doenca; 

STORE predictionData INTO 'enhancedHeartDisease' USING PigStorage(',');



# Etapa 4 - Criação do Modelo Preditivo de Classificação

# Cria a pasta no HDFS
hdfs dfs -mkdir /projeto

# Copia o arquivo gerado pela transformação com o Pig para o HDFS 
hdfs dfs -copyFromLocal enhancedHeartDisease/* /projeto

# Cria um descritor para os dados
mahout describe -p /projeto/part-r-00000 -f /projeto/desc -d 6 N L

# Divide os dados em treino e teste 
mahout splitDataset --input /projeto/part-r-00000 --output /projeto/splitdata --trainingPercentage 0.7 --probePercentage 0.3

# Constrói o modelo RandomForest com uma árvore 
mahout buildforest -d /projeto/splitdata/trainingSet/* -ds /projeto/desc -sl 3 -p -t 1 -o /projeto/model

# Testa o modelo
mahout testforest -i /projeto/splitdata/probeSet -ds /projeto/desc -m /projeto/model -a -mr -o /projeto/predictions


--> Visualizar a Confusion Matrix

# Etapa 5 - Otimização do Modelo Preditivo de Classificação

# Construir o modelo com 25 árvores, a fim de aumentar a acurácia
mahout buildforest -d /projeto/splitdata/trainingSet/* -ds /projeto/desc -sl 3 -p -t 25 -o /projeto/model

# Testa o modelo
mahout testforest -i /projeto/splitdata/probeSet -ds /projeto/desc -m /projeto/model -a -mr -o /projeto/predictions


--> Aumentando o número de árvoresm aumentamos a acurácia do modelo.



