import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)


def Crawl_THPTQG(so_bao_danh):
    so_bao_danh = str(so_bao_danh).rjust(8, '0')
    URL = "https://vietnamnet.vn/giao-duc/diem-thi/tra-cuu-diem-thi-tot-nghiep-thpt/2023/{}.html".format(so_bao_danh)
    r = session.get(URL)
    soup = BeautifulSoup(r.content, 'html.parser')
    target = soup.find('div', attrs={'class': 'resultSearch__right'})
    table = target.find('tbody')
    rows = table.find_all('tr')
    placeHolder = []
    for row in rows:
        lst = row.find_all('td')
        cols = [ele.text.strip() for ele in lst]
        placeHolder.append([ele for ele in cols if ele])
    content = "{},{}\n".format(so_bao_danh, placeHolder)
    return content


if __name__ == "__main__":
    lst = range(1000001, 100000000)
    NUM_WORKERS = cpu_count() * 2
    chunksize = 100000000 // NUM_WORKERS * 4
    pool = Pool(NUM_WORKERS)
    result_iter = pool.imap(Crawl_THPTQG, lst)

    with open("Output.csv", "a", encoding='utf-8') as f:
        for result in result_iter:
            f.write(result)
            print("Đang xử lý điểm của thí sinh số {}".format(result.split(',')[0]))