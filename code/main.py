from leitos import tab_leitos_por_uf, tab_leitos_brasil
from time import time
import os
start_time = time()
################# Execute as funções aqui ################

tab_leitos_brasil()


##########################################################
end_time = time()
execution_time = end_time-start_time
print(f"""Programa executado em {int(execution_time//3600)} horas, 
{int(execution_time//60)} minutos e {int(execution_time%60)} segundos.""".replace('\n',''))