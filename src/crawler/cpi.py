import requests
from bs4 import BeautifulSoup
import pandas as pd
from . import Setting

def crawl_cpi():
    url = "http://www.usinflationcalculator.com/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/"

    r = requests.get(url)
    data = r.text
    soup = BeautifulSoup(data, 'html.parser')

    table = soup.find('table')
    rows = table.tbody.findAll('tr');

    years = []
    cpis = []

    for row in rows:
        year = row.findAll('td')[0].get_text()
        if year.isdigit():
            years.append(int(year))
            if int(year) < 2022:
                cpis.append(float(row.findAll('td')[13].get_text()))
            elif int(year) == 2022:
                month_cpis = [float(n.get_text()) for n in row.findAll('td')[1:3]]
                avg_cpi = sum(month_cpis)/len(month_cpis)
                cpis.append(avg_cpi)

    cpi_table = pd.DataFrame({
        "year": years,
        "avg_annual_cpi": cpis
    })

    cpi_table.to_csv(Setting.CACHE + "cpi.csv", index=False)
    # return cpi_table