import os
from pathlib import Path
from shutil import rmtree
from time import sleep

import numpy as np
import pandas as pd
import pycountry
from entsoe.mappings import Area
from geopy.geocoders import Nominatim
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager


def main():
    country_dict = {}
    for a in Area:
        country_dict[a.name] = a.code

    # deal with United Kingdom unnormal country code
    GB_1 = country_dict["GB"]
    country_dict["GB"] = "10Y1001A1001A92E"
    country_dict["GB_1"] = GB_1

    name_mapping = get_gas_powerplant(
        country_dict, download_path="./test", headless=False
    )

    entsoe_data = []
    for file in tqdm(os.listdir("./test/")):
        entsoe_data.append(
            add_geo_info(pd.read_csv("./test/" + file), name_mapping=name_mapping)
        )
    entsoe_data = pd.concat(entsoe_data)
    entsoe_data = entsoe_data.reset_index(drop=True)

    entsoe_data = entsoe_data.rename(
        {
            "Production Type": "Fueltype",
            "Current Installed Capacity [MW]": "Capacity",
            "country": "Country",
            "Commissioning Date": "DateIn",
            "Decommissioning Date": "DateRetrofit",
        },
        axis=1,
    )

    entsoe_data["Fueltype"] = "Natural Gas"

    entsoe_data.to_csv("output/ENTSOE_Web_database_gas_powerplant.csv", index=False)


def robust_geo_code(query, need_location=False):
    if need_location:
        geolocator = Nominatim(user_agent="gaspowerplantupdate")

        try:
            tmp = geolocator.geocode(query)
        except:
            tmp = None
        return tmp
    else:
        return None


def add_geo_info(dataframe, name_mapping):
    dataframe["country"] = dataframe["country"].apply(lambda x: name_mapping[x])

    locations = (dataframe["Name"] + "," + dataframe["country"]).apply(robust_geo_code)

    dataframe["lat"] = [
        (lambda x: x.latitude if x != None else np.nan)(i) for i in locations
    ]
    dataframe["long"] = [
        (lambda x: x.longitude if x != None else np.nan)(i) for i in locations
    ]
    return dataframe


def find_all_by_country(alpha_2, keys):
    result = []
    for key in keys:
        if alpha_2 in key:
            result.append(key)
    return result


# get full country name


def get_full_country_name(driver, max_try_num=5):
    import re

    count = 0
    current = 0
    name_mapping = {}
    while count < max_try_num:
        try:
            c = driver.find_elements(
                by="xpath", value='//label[@for="{}"]'.format(current)
            )
            c = c[1].text
            short = c.split("(")[-1]
            short = short.strip()[:-1]

            full = c.split("(")[0]
            full = re.sub(r"[^a-zA-Z ]", "", full)
            full = pycountry.countries.search_fuzzy(full)[0].name
            # remove ','
            # full = full.split(',')[0].strip()
            name_mapping[short] = full
            current += 1
        except:
            current += 1
            count += 1

    name_mapping["GB"] = name_mapping["UK"]
    return name_mapping


def query_website(country, bidzone, year, driver):
    query = "https://transparency.entsoe.eu/generation/r2/installedCapacityPerProductionUnit/show?name=&defaultValue=true&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=01.01.{year}+00:00|UTC|YEAR&area.values=CTY|{country}!BZN|{bidzone}&productionType.values=B04"
    query = query.format(country=country, bidzone=bidzone, year=year)
    driver.get(query)


def get_gas_powerplant(
    country_dict,
    download_path="./entsoe_tables",
    headless=True,
    normal_sleep_interval=5,
    download_sleep_interval=3,
):
    # name mapping
    name_mapping = None

    # init webdriver options
    # download path
    if os.path.isdir(download_path):
        rmtree(download_path)
    os.mkdir(download_path)
    abspath = os.path.abspath(download_path)
    number_of_file = len(os.listdir(download_path))

    # init driver
    driver_path = ChromeDriverManager().install()
    chrome_options = Options()

    prefs = {}
    prefs["download.default_directory"] = abspath
    chrome_options.add_experimental_option("prefs", prefs)
    if headless:
        chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

    # init entsoe website
    driver.get("https://transparency.entsoe.eu/")
    sleep(normal_sleep_interval)
    try:
        driver.find_element(
            by="xpath", value="//*[contains(text(), 'I Agree')]"
        ).click()
    except:
        pass

    # login
    try:
        driver.find_element(by="id", value="login-dialog-visible").click()
    except:
        pass
    sleep(2)
    # input username and password
    name = driver.find_element(by="name", value="username")
    name.send_keys("wenruizhou687@gmail.com")
    password = driver.find_element(by="name", value="password")
    password.send_keys("vujkih-nyqne2-Gaccot")
    # login
    driver.find_element(by="name", value="login").click()

    # start download
    # find country codes
    countries = [a for a in country_dict.keys() if "_" not in a]
    for country in tqdm(countries):
        bidzones = find_all_by_country(country, country_dict.keys())
        for bidzone in bidzones:
            # visit target website
            try:
                # query successful
                query_website(
                    country_dict[country],
                    country_dict[bidzone],
                    year=2021,
                    driver=driver,
                )
                sleep(normal_sleep_interval)
                # click download link and download
                driver.find_element(by="id", value="dv-export-data").click()
                # wait until download successful
                while number_of_file == len(os.listdir(download_path)):
                    try:
                        driver.find_element(
                            by="xpath", value='//a[@exporttype="CSV"]'
                        ).click()
                    except:
                        continue
                    sleep(download_sleep_interval)
                number_of_file = len(os.listdir(download_path))

                # download finishing

                # rename downloaded file:
                new_file = sorted(Path(download_path).iterdir(), key=os.path.getmtime)[
                    -1
                ]
                # add country code and remove leer file:
                df = pd.read_csv(new_file)
                if len(df) > 0:
                    df["country"] = country
                    df.to_csv(
                        download_path + "/{}{}.csv".format(country, bidzone),
                        index=False,
                    )

                    # unitl here, the table certainly successfully download
                    # check if need to download country name mapping
                    if name_mapping == None:
                        name_mapping = get_full_country_name(driver)

                os.remove(new_file)

            except:
                # query fail
                continue
                # avoid being blocked by server
                sleep(normal_sleep_interval)

    driver.close()
    return name_mapping


if __name__ == "__main__":
    main()
