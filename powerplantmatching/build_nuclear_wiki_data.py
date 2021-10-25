# import crawler packages
from time import sleep

import numpy as np
import pandas as pd

# import other third party packages
import pycountry
from bs4 import BeautifulSoup as BS
from googletrans import Translator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm
from webdriver_manager.chrome import (  # this package make sure code works for every PC
    ChromeDriverManager,
)

# add progress_apply to pandas
tqdm.pandas()

# import python built-in packages
import re


def main():
    ####################################################################################
    # init
    ####################################################################################
    # automatically setup of web driver parameter
    driver_path = ChromeDriverManager().install()

    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # open a web driver
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

    ####################################################################################
    # get raw wiki table from: 'https://de.wikipedia.org/wiki/Liste_der_Kernkraftwerke_in_Europa'
    ####################################################################################
    eu_list = []  # main variable for storing nuclear power plant data

    # download website source code wiki
    driver.get("https://de.wikipedia.org/wiki/Liste_der_Kernkraftwerke_in_Europa")
    # wait website loading
    sleep(5)
    # select source code that related to tables
    country = driver.find_element(by="xpath", value='//*[@id="toc"]/ul/li[1]/ul')

    tables_list = driver.find_elements_by_xpath(
        "//table[@class = 'wikitable sortable jquery-tablesorter']"
    )
    # get html code of tables and store them in list
    tables = [i.get_attribute("innerHTML") for i in tables_list]
    # get more accurate table column from pandas html reader
    tables_pd = pd.read_html(
        "https://de.wikipedia.org/wiki/Liste_der_Kernkraftwerke_in_Europa", header=0
    )

    # parse html source code
    for table in tqdm(tables):
        # convert to bs object
        soup = BS(table, "html.parser")
        columns, content = process_table(soup)

        eu_list.append(pd.DataFrame(content, columns=columns))

    # number of useful columns
    column_selection = [0, 1, 2, 4, 5, 6, 9, 10]

    # combine pandas html reader result and html source code parsing
    for i in range(len(eu_list)):
        if eu_list[i].Name.__len__() != tables_pd[i].Name.__len__():
            tmp_columns = tables_pd[i].iloc[0]
            tmp = eu_list[i].copy()
            tmp.columns = tmp_columns

            tmp = tmp.iloc[:, :-1].copy()
            tmp.reset_index(drop=True, inplace=True)
        else:
            tmp = eu_list[i].iloc[:, column_selection].copy()

        # process irregular date (datein and dateretrofit)
        tmp.iloc[:, -2] = tmp.iloc[:, -2].apply(deal_with_format_problem)
        tmp.iloc[:, -1] = tmp.iloc[:, -1].apply(deal_with_format_problem)

        # store table in eu_list
        eu_list[i] = tmp

    ####################################################################################
    # add country and geo coordinate info
    ####################################################################################

    # get the countries list of wiki tables, it has same order as tables in eu_list
    country = country.get_attribute("innerHTML")
    clist = [i[1:].split('"')[0] for i in re.findall('#.+"', country)]

    # remove countries that don't have nuclear power plant
    clist.remove("Italien")
    clist.remove(
        "Polen"
    )  # here is very risky, if wiki page changes, the code may not work
    clist.remove("Österreich")
    # translate from germany to english:
    clist = pd_column_translate(clist)

    # add country and geo coordinate info to eu_list
    tmp = []
    for idx in tqdm(range(len(eu_list))):
        eu_list[idx]["Country"] = pycountry.countries.search_fuzzy(clist[idx])[0].name
        geo = pd.DataFrame(get_geo(eu_list[idx].Name, driver=driver))
        tmp.append(pd.concat([eu_list[idx].iloc[:, 1:], geo], axis=1))
    eu_list = tmp

    ####################################################################################
    # detail optimization
    ####################################################################################

    # translate columns name
    uni_columns_trans = list(eu_list[0].columns)
    # reorder columns
    new_order = uni_columns_trans[-1:] + uni_columns_trans[:-1]
    tmp = []
    for i in range(len(eu_list)):
        eu_list[i].columns = uni_columns_trans
        tmp_df = eu_list[i][new_order].copy()
        tmp_df.fillna(method="ffill", inplace=True)
        tmp.append(tmp_df)
    eu_list = tmp

    # combine all data to a single dataframe
    df = pd.concat(eu_list)
    # gross performance seems to be unimportant, drop it
    df = df.drop(["Brutto-leistungin MW", "Status"], axis=1)

    # rename columns follow the way in powerplantmatching repository
    namemap = {
        "Name": "Name",
        "Block": "Block",
        "Reaktortyp": "Technology",
        "Netto-leistungin MW": "Capacity",
        "Kommer-zieller Betrieb(geplant)": "DateIn",
        "Abschal-tung(geplant)": "DateRetrofit",
    }
    df = df.rename(columns=namemap)

    # add fueltype
    df["Fueltype"] = "Nuclear"
    # print(df.columns)
    # convert date to year
    df["DateIn"] = df["DateIn"].apply(lambda x: to_year(x, True))
    df["DateRetrofit"] = df["DateRetrofit"].apply(to_year)

    ####################################################################################
    # update dataset with extra data
    #     1. france wiki data: https://fr.wikipedia.org/wiki/Industrie_nucléaire_en_France
    #         we already store it in a local csv in the same directory of this script
    #         file name: france.csv
    #
    #     2. manually collected data by IAI group, we marked out data source in csv file
    #         we already store it in a local csv in the same directory of this script
    #         file name: new_powerplant_checked.xls
    ####################################################################################

    # load extra france data
    france = pd.read_csv("package_data/france.csv")
    france["Nom du réacteur"] = france["Nom du réacteur"].str.replace(
        r"\[.+\]|St-", "", regex=True
    )
    france = france.set_index("Nom du réacteur")
    # update dataset with extra france data
    df["DateRetrofit"] = df.apply(lambda x: new_date(x, france), axis=1)

    # load extra information of power plant that are under construction
    under_construction = pd.read_excel("package_data/new_powerplant_checked.xls")
    # update dataset with extra information of power plant that are under construction
    df = df[df["DateIn"].notna()]
    df = pd.concat([df, under_construction])
    df.reset_index(drop=True, inplace=True)

    # translate German to English
    df["Technology"] = translate(df["Technology"])

    # remove special symbols in the dataset
    for column in df.columns:
        if df[column].dtype != "float64" and column != "source":
            df[column] = df[column].str.replace(r"\[.+\]", "", regex=True)
            df[column] = df[column].str.replace(r"\xa0", "", regex=True)
            df[column] = df[column].str.replace(r"^[-–]", "", regex=True)

    driver.quit()
    # remove special symbols in the dataset
    df["DateIn"] = (
        df["DateIn"].str.split(r"[–-]").str[-1].replace("", np.nan)
    )  # use the latest comission date
    df["DateRetrofit"] = (
        df["DateRetrofit"].str.split(r"[–-]").str[-1].replace("", np.nan)
    )
    # use the lastest decomission estimation
    df["Capacity"] = df["Capacity"].str.replace(
        "\.", ""
    )  # deal with germany number representation
    df["Capacity"] = df["Capacity"].str.replace(r"\[.+\]", "")
    df["Capacity"] = df["Capacity"].replace("-", np.nan)
    df["Capacity"] = df["Capacity"].replace("", np.nan)
    df["Capacity"] = df["Capacity"].astype(float)
    return df


# =================
# function of process_table
# =================


def process_table(table):
    """
    extract table from input html file (table)

    Inputs
    ----------
    table: str
    html file of the table that we want convert to pandas dataframe

    Outputs
    -------
    columns: list
    a list of column names of the table

    content: list
    content of the table
    a list of list, which is suitable for pandas dataframe creation

    """
    thead = table.find("thead").find("tr")
    tbody = table.find_all("tbody")[0].find_all("tr")
    # first row is head
    columns = process_head2list(thead)

    content = []
    # process the longest row
    content.append(process_tbody2row(tbody[0]))

    columns += ["None"] * (len(content[0]) - len(columns))

    for row in tbody[1:]:
        content.append(process_tbody2row(row, len(content[0])))
    return columns, content


# process to get the head
def process_head2list(head):
    """
    extract column name list from html string (head)

    Inputs
    ----------
    head: str
    a string of table heads in html format

    Outputs
    -------
    head_list: list
    list of table's column names
    """
    head_list = []
    for th in head.find_all("th"):
        head_list.append("".join(th.strings).strip())
        if head_list[-1] == "Leistung (MW)":
            head_list.pop()
            head_list.append("Netto Leistung")
            head_list.append("Brutto Leistung")
    return head_list


def process_tbody2row(row, length=0):
    """
    extract content of a row of the table from html string (row)

    Inputs
    ----------
    row: str
    one row of the table we want to parse

    length:
    the number of columns of a table.
    In html,the row will not maintain the position of nan values.
    E.g., if there are 10 columns in the table, in the row there 4 nan values, then the row only have 6 elements.
    But position information is important for later process.
    Therefore we need the length of the table to make each row have same length (10)

    Output
    -------
    row_list: list
    a list version of the input row. Each element in the list is the value of a column in that row

    """
    row_list = []
    need_link = True  # only get the link of first column
    for td in row.find_all("td"):
        if isinstance(td.contents[0], str):
            # check if a cell has link or not
            cell = " ".join(td.find_all(text=True)).strip()
            # content
        else:
            if need_link:
                cell = td.find_all("a")[0]
                # (content, link)
                cell = (" ".join(td.find_all(text=True)).strip(), cell["href"])
            else:
                cell = " ".join(td.find_all(text=True))
        # after first column, we don't need link anymore
        need_link = False
        row_list.append(cell)
    # solve merge row problem if content shorter
    difference = length - len(row_list)
    if difference > 0:
        if difference == 1:
            row_list = [None] + row_list
        else:
            row_list = [None, row_list[0]] + [None] * (difference - 1) + row_list[1:]
    return row_list


######


def deal_with_format_problem(x):
    """

    Input
    ----------
    x: str
    number of year

    Output
    -------
    year in format 'yyyy'

    """
    if re.findall(r"veraltet", x):
        return "veraltet"
    m = re.findall(r"[0-9]{2}\.[0-9]{2}\.[0-9]{4}", x)
    if not m:
        return x
    else:
        return m[0]


def get_location(link, driver):
    """
    get the geographical coordinate of a nuclear power plant from the input link.
    It is the basis of next function: get_geo(series, driver, need_country=False)

    Inputs
    ----------
    link: str
    link to the website that contains lat and long

    driver: object
    crawler object

    Outputs
    -------
    coordinate: {'lat': float,'long': float}
    country name: str
    """
    # this script only work for deutsch version website

    # get website
    driver.get("https://de.wikipedia.org/" + link)

    # find script
    script = driver.find_elements_by_xpath("//script")
    # to string
    script = script[0].get_attribute("innerHTML")
    # use re find the content we need

    x = re.findall(r'{"lat":[ \n0-9.]+,"lon":[ \n0-9.]+}', str(script))
    country = re.findall(r'"Kernkraftwerk in [a-zA-Z]+"', str(script))
    # print(country)

    if x:
        country_name = None
        for i in country:
            country_name = eval(i).split(" ")[-1]
            if country_name.lower() != "europa":
                break
            else:
                country_name = None

        return eval(x[0]), country_name
    else:
        return None


def get_geo(series, driver, need_country=False):
    """
    get the geographical coordinate of a series of nuclear power plants

    Inputs
    ----------
    series: list
    list of links, which store coordinate information

    driver: object

    need_country: boolean
    True: output will contain country names
    False: output will not contain country names

    Outputs
    -------
    result: list
    list of dict, which can be converted to pandas dataframe
    """
    geo = {}
    result = []
    for unit in series.to_list():
        if unit:
            if unit[0] not in geo:
                content = get_location(unit[1], driver)
                if content:
                    location, country = content
                    geo[unit[0]] = location
                    geo[unit[0]]["country"] = country
                else:
                    location = None
                    geo[unit[0]] = location

            if geo[unit[0]]:
                tmp = geo[unit[0]].copy()
            else:
                tmp = {"lat": None, "lon": None, "country": None}
            tmp["Name"] = unit[0]
            result.append(tmp)
        else:
            result.append({"Name": None, "lat": None, "lon": None, "country": None})
    if not need_country:
        for element in result:
            del element["country"]
    return result


def pd_column_translate(column_series):
    """
    translate strings in a list to English strings

    Inputs
    ----------
    column_series: list
    list of string wait to be translated

    Returns
    -------
    list of string that are translated to English

    """
    cont = column_series
    if not isinstance(column_series, list):
        cont = column_series.to_list()
    cont = [c.replace("_", " ") for c in cont]
    translator = Translator(service_urls=["translate.google.com"])
    x = [robust_string_translate(g, translator).text for g in cont]
    return x


def translate(series):
    """
    translate each string in a pandas series to English

    Inputs
    ----------
    series: pandas.Series
    series that wait to be translated

    Outputs
    -------
    translated pandas.Series

    """
    translator = Translator(service_urls=["translate.google.com"])
    tmp = series.progress_apply(lambda x: robust_string_translate(x, translator))
    return tmp.apply(lambda x: x.text if x.src == "de" else x.origin)


def robust_string_translate(string, translator):
    """
    translate a string to English string robustly.
    It is the basis of function: translate() and pd_column_translate()

    Inputs
    ----------
    string: str
    string wait to be translated

    translator: object
    googletrans.Translator

    Outputs
    -------
    translated string

    """
    from time import sleep

    translate_result = string
    for i in range(10):
        try:
            translate_result = translator.translate(string)
            break
        except:
            sleep(3)
    return translate_result


def to_year(x, notmatchNaN=False):
    """
    extract year (format: yyyy) from the string

    Inputs
    ----------
    x: str
    a string contain year(e.g., '1986-01-02','1986[4]')

    notmatchNaN: boolean
    if the string does not contain year (e.g., '01-02'), this parameter decides
    what will be the output
    True:  return  None
    False: return the the input string x

    Outputs
    -------
    a year, or the placeholder decided by notmatchNaN

    """
    m = re.findall("[0-9]{4}", x)
    if m:
        return m[0]
    else:
        if notmatchNaN:
            return None
        else:
            return x


def new_date(x, france):
    """
    update the decomission date of nuclear power plants in France

    Inputs
    ----------
    x: pandas.Series
    a row in pandas dataframe, that records the information of a nuclear power plant

    france: pandas.DataFrame
    a table that contains new decomission dates of nuclear power plant in France

    Outputs
    -------
    new decomission date: str
    (e.g., '2030-2035', '2030')

    """
    key = x["Name"].replace(r"\(.*\)|\[.*\]|Saint-", "") + "-" + x["Block"]
    try:
        date = france.loc[key, "Arrêt définitif prévu[10],[11]"]
    except:
        date = x["DateRetrofit"]
    return date


if __name__ == "__main__":
    df = main()
    df.to_csv("new_nuclear_data.csv", index=False)
