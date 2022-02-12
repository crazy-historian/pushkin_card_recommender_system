import urllib.request
import json
import config as cfg

final_list = []

def infinite_sequence():
    num = 0
    while True:
        yield num
        num += 100

def info_collecting():
    for i in infinite_sequence():
        with urllib.request.urlopen("https://pro.culture.ru/api/2.5/events/?offset="+str(i)+"&limit=100&fields=category%2CextraFields%2Ctags%2Cend&sort=_id&apiKey=mbn8puyjl4w6q86915kn") as url:
            events_list = json.loads(url.read().decode())['events']
            for j in events_list:
                final_list.append(j)
        if len(events_list) != 100:
            break
    return final_list


if __name__ == "__main__":

    with open(cfg.REQUEST_FILE_PATH, 'w', encoding='utf-8') as json_file:
        json.dump(info_collecting(), json_file, ensure_ascii=False)