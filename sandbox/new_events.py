
import pandas as pd
import numpy as np

import json   

path_input = input('Введите путь к исходным файлам: ')
path_output = input('Введите путь к итоговым файлам: ')

with open(path_input + '/rec_test.json') as f:
    recomindations = json.load(f)

events_set = set()
for i in recomindations.values():
    z = list(i.keys())
    for y in z:
        events_set.add(y)
        
events = pd.read_csv(path_input + '/events.csv', sep = ';', error_bad_lines=False)
events['ID'] = events['ID'].apply(lambda x: str(x))

new_events = list(events['ID'].unique())

result=list(set(events_set) - set(new_events))

for i in recomindations.keys():
    for j in result:
        try:
            del recomindations[i][j]
        except KeyError:
            continue
            
with open(path_output + '/rec_test_2.json', 'w') as outfile:
    json.dump(recomindations, outfile)
