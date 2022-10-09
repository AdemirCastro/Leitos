from beds import bed_tab_by_uf, brazil_bed_tab
from time import time
import os
start_time = time()
################# Run your code here ################

brazil_bed_tab()

#####################################################
end_time = time()
execution_time = end_time-start_time
print(f"""Program executed in {int(execution_time//3600)} hours, 
{int(execution_time//60)} minutes and {int(execution_time%60)} seconds.""".replace('\n',''))