## uso das bibliotecas pandas e pyarrow para carregar e manipular dados de um arquivo .parquet da URL fornecida
## acessa uma pasta do banco, lê e converte em tabela
## contém informações relacionadas a atendimentos/procedimentos médicos

pip install pandas
Requirement already satisfied: pandas in /usr/local/lib/python3.10/dist-packages (1.5.3)
Requirement already satisfied: python-dateutil>=2.8.1 in /usr/local/lib/python3.10/dist-packages (from pandas) (2.8.2)
Requirement already satisfied: pytz>=2020.1 in /usr/local/lib/python3.10/dist-packages (from pandas) (2022.7.1)
Requirement already satisfied: numpy>=1.21.0 in /usr/local/lib/python3.10/dist-packages (from pandas) (1.22.4)
Requirement already satisfied: six>=1.5 in /usr/local/lib/python3.10/dist-packages (from python-dateutil>=2.8.1->pandas) (1.16.0)

import pandas as pd

dataframe = pd.read_parquet('/content/ATEN_ATENDIMENTOS.parquet')

print (dataframe)
 USUA_ID    IDPR_ID  ESPE_ID   CONV_ID      COCV_ID    cont_id  \
0       193473995  193474296    30645   3433588  150110239.0  150110231   
1       198925319  198925589    30605   3433588  250838919.0  154718124   
2       101530148  101530514    30629  11891175          NaN  155279246   
3       193473995  193474296    30645   3433588  153351385.0  153351379   
4        59304569   59304575    30647   3433768  149904844.0   60955671   
...           ...        ...      ...       ...          ...        ...   
126351  477692553  477692752    30633   3433588  446380367.0  154162171   
126352  478426731  478427678    30629   3433588  153586185.0  153586177   
126353  518441295  518442107    30633   3433588  154488913.0  154488903   
126354  518497197  518499241    30626   3433588  154417031.0  154417021   
126355  478431649  478432266    30615   3433588  153558118.0  153558108   

               id      marc_id             abertura          finalizacao  np  \
0       250493216  227783029.0  2022-03-31 14:56:10  2022-03-31 15:34:12   0   
1       250841336  244319367.0  2022-04-01 11:17:09  2022-04-01 12:16:28   0   
2       250988394  250946399.0  2022-04-01 16:13:25  2022-04-01 16:23:00   0   
3       252347826  252267746.0  2022-04-04 19:15:21  2022-04-04 19:31:21   0   
4       199631017  199529247.0  2021-11-04 10:21:16  2022-04-04 14:04:31   0   
...           ...          ...                  ...                  ...  ..   
126351  511457422  508460024.0  2023-03-08 14:41:48  2023-03-08 15:04:37   0   
126352  510821561  505422241.0  2023-03-07 16:19:41  2023-03-07 16:31:36   0   
126353  526514928  523819951.0  2023-03-31 07:24:22  2023-03-31 07:27:58   0   
126354  526094057  523066083.0  2023-03-30 14:40:49  2023-03-30 14:46:40   0   
126355  526202856  522233327.0  2023-03-30 15:48:50  2023-03-30 15:52:21   0   

        numero    loat_id  
0            1  193467067  
1            1  198908584  
2            1  101531155  
3            1  193467067  
4            2   70656747  
...        ...        ...  
126351       1  477887869  
126352       4  478471455  
126353       2  517023595  
126354       2  518254053  
126355       4  499635216  

[126356 rows x 13 columns]

pip install pandas pyarrow

Requirement already satisfied: pandas in /usr/local/lib/python3.10/dist-packages (1.5.3)
Requirement already satisfied: pyarrow in /usr/local/lib/python3.10/dist-packages (9.0.0)
Requirement already satisfied: python-dateutil>=2.8.1 in /usr/local/lib/python3.10/dist-packages (from pandas) (2.8.2)
Requirement already satisfied: pytz>=2020.1 in /usr/local/lib/python3.10/dist-packages (from pandas) (2022.7.1)
Requirement already satisfied: numpy>=1.21.0 in /usr/local/lib/python3.10/dist-packages (from pandas) (1.22.4)
Requirement already satisfied: six>=1.5 in /usr/local/lib/python3.10/dist-packages (from python-dateutil>=2.8.1->pandas) (1.16.0)

import pyarrow.parquet as pq

import requests

url_arquivo = 'https://objectstorage.sa-saopaulo-1.oraclecloud.com/p/BXnO6hywmppiZn64tBDrWXtpzg-7Ic8NqKCCvNOQon789uWZGWxf8XTlLqMDJK_L/n/gr6tytxycmi3/b/integracao_prd/o/PEP/DadosPEP/ATEN_ATENDIMENTOS.parquet'

response = requests.get(url_arquivo)

with open ("ATEN_ATENDIMENTOS.parquet","wb") as f: f.write(response.content)

table = pq.read_table("ATEN_ATENDIMENTOS.parquet")
print(table.to_pandas())

 USUA_ID    IDPR_ID  ESPE_ID  CONV_ID      COCV_ID    cont_id  \
0       138957013  138957772    30626  3433755  152373717.0  152373713   
1       184967296  184967302    30605  3433594  150188542.0  150188534   
2       198925319  198925589    30605  3433588  152966622.0  152966612   
3       248130641  248134383    30661  3433697  217138553.0  211716707   
4       184967296  184967302    30605  3433594  149890983.0  149890975   
...           ...        ...      ...      ...          ...        ...   
128685  502939208  502939649    30615  3433588  154052345.0  154052337   
128686  482320009  482325136    30649  3433588  446769713.0  446769707   
128687  502583795  502584611    30652  3433588  446678327.0  446678323   
128688  482174085  482175909    30615  3433588  483490095.0  483490089   
128689   94635265   94635301    30671  3433556  156173436.0  156131319   

               id      marc_id             abertura          finalizacao  np  \
0       250391229  250132022.0  2022-03-31 11:34:58  2022-03-31 12:06:05   0   
1       250536871  249704121.0  2022-03-31 15:44:53  2022-03-31 16:06:38   0   
2       250475629  238505126.0  2022-03-31 14:32:20  2022-03-31 15:06:11   0   
3       250797593  248638331.0  2022-04-01 10:20:14  2022-04-05 08:38:24   0   
4       252313652  252263536.0  2022-04-04 18:31:09  2022-04-04 18:31:52   0   
...           ...          ...                  ...                  ...  ..   
128685  536155118  531693212.0  2023-04-14 14:27:38  2023-04-14 14:33:48   0   
128686  535817520  529092230.0  2023-04-14 09:33:08  2023-04-14 09:36:50   0   
128687  536155108  535225842.0  2023-04-14 14:27:37  2023-04-14 14:29:04   0   
128688  487439300  486913358.0  2023-01-23 12:22:55  2023-01-23 12:27:07   0   
128689  444573688  439747616.0  2022-10-19 10:56:30  2022-10-19 11:00:15   0   

        numero    loat_id  
0            1  226987740  
1            1  184967308  
2            1  198908584  
3            3  188088842  
4            1  184967308  
...        ...        ...  
128685       3  499635216  
128686       1  499635216  
128687       1  499635216  
128688       1  482463498  
128689      58   94635171  

[128690 rows x 13 columns]

