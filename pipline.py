#Pipline

import os

#предобработка
os.system('python preprocessing.py')

#подготовка региональных датасетов
os.system('python prep.py')

#подготовка рекомендаций по регионам
os.system('python main.py')