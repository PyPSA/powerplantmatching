# Data changes from v0.9.0 to v0.9.1

## Overview

Total capacity and number of entries of both versions, with the change in absolute terms and in % of the old version. The heatmap shows the change per country and fuel type, the tables the data sources and the status.

The total capacity changes from 1 450.4 GW to 1 619.8 GW (+169.4 GW). The number of entries changes from 172 961 to 302 301 (+129 340).

<div style="position: relative; margin-bottom: 1em">
<p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
<iframe src="overview/total.html" title="Total" loading="lazy"
  style="position: relative; display: block; width: 100%; height: 156px; border: 0"></iframe>
</div>

| Quantity | old | new | delta |
| :-- | --: | --: | --: |
| Capacity [GW] | 1 450.431 | 1 619.809 | +169.379 (+11.7%) |
| Entries | 172 961 | 302 301 | +129 340 (+74.8%) |

Capacities are the sum over all entries in the file, including planned and retired plants, in GW. The delta in brackets is in % of the old version, left out if the old version has none.
{: .table-notes }

### By country and fuel type

The pairs of country and fuel type with the largest capacity change are Germany / Wind (+103.1 GW, +30 692 entries), Germany / Solar (+15.7 GW, +72 280 entries), Germany / Biogas (+4.7 GW, +13 395 entries).

<div style="position: relative; margin-bottom: 1em">
<p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
<iframe src="overview/country_fueltype.html" title="Change by country and fuel type" loading="lazy"
  style="position: relative; display: block; width: 100%; height: 1002px; border: 0"></iframe>
</div>

The colour shows the size of the change in classes of one decade, the largest from one decade below the decade of the largest change, so small changes stay visible. Blank cells have entries in neither version. Hover a cell for old, new and delta. The table is in [overview/country_fueltype.csv](overview/country_fueltype.csv).
{: .table-notes }

### Sources

<div style="position: relative; margin-bottom: 1em">
<p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
<iframe src="overview/sources.html" title="Sources" loading="lazy"
  style="position: relative; display: block; width: 100%; height: 332px; border: 0"></iframe>
</div>

| Source | Capacity old [GW] | Capacity new [GW] | Capacity delta [GW] | Entries old | Entries new | Entries delta |
| :-- | --: | --: | --: | --: | --: | --: |
| BEYONDCOAL | 238.238 | 238.826 | +0.588 (+0.2%) | 359 | 362 | +3 (+0.8%) |
| EESI | 82.174 | 82.298 | +0.124 (+0.2%) | 1 123 | 1 143 | +20 (+1.8%) |
| ENTSOE | 527.374 | 527.374 | +0 (+0%) | 1 138 | 1 138 | +0 (+0%) |
| GEM | 1 258.374 | 1 295.895 | +37.521 (+3%) | 20 920 | 23 625 | +2 705 (+12.9%) |
| GEO | 403.867 | 405.217 | +1.35 (+0.3%) | 930 | 935 | +5 (+0.5%) |
| GHR | 189.758 | 189.848 | +0.09 (+0%) | 2 459 | 2 460 | +1 (+0%) |
| GPD | 405.04 | 406.267 | +1.227 (+0.3%) | 4 581 | 4 634 | +53 (+1.2%) |
| JRC | 193.156 | 194.283 | +1.127 (+0.6%) | 2 788 | 2 802 | +14 (+0.5%) |
| MASTR | 226.66 | 358.482 | +131.822 (+58.2%) | 151 090 | 277 756 | +126 666 (+83.8%) |
| OPSD | 471.296 | 472.462 | +1.166 (+0.2%) | 2 993 | 3 018 | +25 (+0.8%) |
| OSM | 420.319 | 421.537 | +1.218 (+0.3%) | 5 964 | 6 007 | +43 (+0.7%) |

Capacity and entries per data source. An entry with several sources counts for each, so the sources add up to more than the total.
{: .table-notes }

### Status in 2026

<div style="position: relative; margin-bottom: 1em">
<p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
<iframe src="overview/status.html" title="Status in 2026" loading="lazy"
  style="position: relative; display: block; width: 100%; height: 178px; border: 0"></iframe>
</div>

| Status | Capacity old [GW] | Capacity new [GW] | Capacity delta [GW] |
| :-- | --: | --: | --: |
| operating | 1 112.373 | 1 243.247 | +130.874 (+11.8%) |
| planned | 32.486 | 63.203 | +30.717 (+94.6%) |
| retired | 230.352 | 235.972 | +5.62 (+2.4%) |
| undated | 75.22 | 77.388 | +2.168 (+2.9%) |

Capacity by status in 2026: operating (DateIn in or before 2026, no DateOut or DateOut in or after 2026), planned (DateIn after 2026), retired (DateOut before 2026) and undated (no DateIn).
{: .table-notes }

## Map

The entries with coordinates of both versions, each loaded on a button click. Switch between the versions, the entries only in one version (matched by name, country, fuel type and position) and the capacity change per grid cell.

<div style="position: relative; margin-bottom: 1em">
<p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
<iframe src="map.html" title="Map of all entries" loading="lazy"
  style="position: relative; display: block; width: 100%; height: 600px; border: 0"></iframe>
</div>

[Open the map in a full window](map.html)

## By country

The change per country, with one tab per table. A figure shows old, new and change, the table below it is collapsed and sorted by a click on a column header.

The countries with the largest capacity change are Germany (+131.7 GW, +126 667 entries), Spain (+5.9 GW, +464 entries), Bulgaria (+3.5 GW, +167 entries).

=== "Capacity"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_country/capacity.html" title="By country: Capacity" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 882px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Country | Capacity old [GW] | Capacity new [GW] | Capacity delta [GW] |
        | :-- | --: | --: | --: |
        | Albania | 2.705 | 2.708 | +0.003 (+0.1%) |
        | Austria | 26.303 | 27.622 | +1.319 (+5%) |
        | Belgium | 23.658 | 24.2 | +0.542 (+2.3%) |
        | Bosnia and Herzegovina | 5.448 | 5.578 | +0.13 (+2.4%) |
        | Bulgaria | 18.675 | 22.191 | +3.517 (+18.8%) |
        | Croatia | 5.15 | 5.505 | +0.356 (+6.9%) |
        | Czechia | 21.185 | 22.138 | +0.953 (+4.5%) |
        | Denmark | 16.529 | 17.588 | +1.059 (+6.4%) |
        | Estonia | 2.342 | 2.826 | +0.484 (+20.7%) |
        | Finland | 30.324 | 30.379 | +0.055 (+0.2%) |
        | France | 157.667 | 158.571 | +0.904 (+0.6%) |
        | Germany | 269.308 | 401.036 | +131.727 (+48.9%) |
        | Greece | 36.406 | 38.427 | +2.021 (+5.6%) |
        | Hungary | 11.141 | 12.263 | +1.122 (+10.1%) |
        | Ireland | 16.837 | 17.127 | +0.29 (+1.7%) |
        | Italy | 109.841 | 110.549 | +0.708 (+0.6%) |
        | Kosovo | 1.834 | 1.847 | +0.013 (+0.7%) |
        | Latvia | 3.458 | 3.668 | +0.21 (+6.1%) |
        | Lithuania | 9.067 | 9.152 | +0.084 (+0.9%) |
        | Luxembourg | 1.54 | 1.544 | +0.005 (+0.3%) |
        | Moldova | 5.279 | 5.318 | +0.038 (+0.7%) |
        | Montenegro | 1.076 | 1.078 | +0.003 (+0.3%) |
        | Netherlands | 45.075 | 48.473 | +3.398 (+7.5%) |
        | North Macedonia | 2.813 | 2.965 | +0.152 (+5.4%) |
        | Norway | 39.417 | 40.779 | +1.362 (+3.5%) |
        | Poland | 67.052 | 69.305 | +2.253 (+3.4%) |
        | Portugal | 26.443 | 26.954 | +0.511 (+1.9%) |
        | Romania | 32.922 | 34.86 | +1.938 (+5.9%) |
        | Serbia | 8.555 | 9.974 | +1.419 (+16.6%) |
        | Slovakia | 9.339 | 9.946 | +0.607 (+6.5%) |
        | Slovenia | 4.27 | 4.307 | +0.037 (+0.9%) |
        | Spain | 142.615 | 148.501 | +5.886 (+4.1%) |
        | Sweden | 51.85 | 54.422 | +2.572 (+5%) |
        | Switzerland | 22.012 | 22.365 | +0.353 (+1.6%) |
        | Ukraine | 57.696 | 58.396 | +0.7 (+1.2%) |
        | United Kingdom | 164.601 | 167.248 | +2.648 (+1.6%) |

=== "Entries"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_country/entries.html" title="By country: Entries" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 882px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Country | Entries old | Entries new | Entries delta |
        | :-- | --: | --: | --: |
        | Albania | 48 | 49 | +1 (+2.1%) |
        | Austria | 398 | 487 | +89 (+22.4%) |
        | Belgium | 244 | 334 | +90 (+36.9%) |
        | Bosnia and Herzegovina | 61 | 87 | +26 (+42.6%) |
        | Bulgaria | 258 | 425 | +167 (+64.7%) |
        | Croatia | 79 | 120 | +41 (+51.9%) |
        | Czechia | 466 | 681 | +215 (+46.1%) |
        | Denmark | 290 | 383 | +93 (+32.1%) |
        | Estonia | 76 | 177 | +101 (+132.9%) |
        | Finland | 424 | 426 | +2 (+0.5%) |
        | France | 3 031 | 3 170 | +139 (+4.6%) |
        | Germany | 151 304 | 277 971 | +126 667 (+83.7%) |
        | Greece | 1 685 | 2 074 | +389 (+23.1%) |
        | Hungary | 462 | 626 | +164 (+35.5%) |
        | Ireland | 232 | 235 | +3 (+1.3%) |
        | Italy | 1 143 | 1 157 | +14 (+1.2%) |
        | Kosovo | 7 | 9 | +2 (+28.6%) |
        | Latvia | 81 | 91 | +10 (+12.3%) |
        | Lithuania | 129 | 146 | +17 (+13.2%) |
        | Luxembourg | 32 | 33 | +1 (+3.1%) |
        | Moldova | 40 | 48 | +8 (+20%) |
        | Montenegro | 10 | 12 | +2 (+20%) |
        | Netherlands | 672 | 809 | +137 (+20.4%) |
        | North Macedonia | 68 | 96 | +28 (+41.2%) |
        | Norway | 1 045 | 1 064 | +19 (+1.8%) |
        | Poland | 2 746 | 2 762 | +16 (+0.6%) |
        | Portugal | 613 | 646 | +33 (+5.4%) |
        | Romania | 414 | 617 | +203 (+49%) |
        | Serbia | 63 | 70 | +7 (+11.1%) |
        | Slovakia | 122 | 233 | +111 (+91%) |
        | Slovenia | 64 | 73 | +9 (+14.1%) |
        | Spain | 2 584 | 3 048 | +464 (+18%) |
        | Sweden | 673 | 684 | +11 (+1.6%) |
        | Switzerland | 614 | 625 | +11 (+1.8%) |
        | Ukraine | 178 | 179 | +1 (+0.6%) |
        | United Kingdom | 2 605 | 2 654 | +49 (+1.9%) |

=== "Completeness"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_country/completeness.html" title="By country: Completeness" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 882px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Country | Complete old [%] | Complete new [%] | Complete delta [pp] |
        | :-- | --: | --: | --: |
        | Albania | 66.7 | 67.3 | +0.7 |
        | Austria | 92.2 | 93.6 | +1.4 |
        | Belgium | 78.3 | 84.1 | +5.9 |
        | Bosnia and Herzegovina | 98.4 | 98.9 | +0.5 |
        | Bulgaria | 87.2 | 91.8 | +4.6 |
        | Croatia | 91.1 | 94.2 | +3 |
        | Czechia | 95.9 | 97.2 | +1.3 |
        | Denmark | 88.6 | 91.4 | +2.8 |
        | Estonia | 89.5 | 95.5 | +6 |
        | Finland | 63 | 63.1 | +0.2 |
        | France | 95.2 | 95.3 | +0.1 |
        | Germany | 45.7 | 51.9 | +6.2 |
        | Greece | 76.8 | 80.3 | +3.5 |
        | Hungary | 96.3 | 97.3 | +1 |
        | Ireland | 79.7 | 79.6 | -0.2 |
        | Italy | 57.6 | 57.6 | +0 |
        | Kosovo | 85.7 | 88.9 | +3.2 |
        | Latvia | 86.4 | 87.9 | +1.5 |
        | Lithuania | 93.8 | 94.5 | +0.7 |
        | Luxembourg | 31.2 | 30.3 | -0.9 |
        | Moldova | 97.5 | 97.9 | +0.4 |
        | Montenegro | 80 | 83.3 | +3.3 |
        | Netherlands | 87.2 | 89.2 | +2 |
        | North Macedonia | 91.2 | 93.8 | +2.6 |
        | Norway | 98.7 | 98.6 | -0.1 |
        | Poland | 12.5 | 12.7 | +0.3 |
        | Portugal | 94.5 | 94.6 | +0.1 |
        | Romania | 81.4 | 87.5 | +6.1 |
        | Serbia | 90.5 | 90 | -0.5 |
        | Slovakia | 95.9 | 97.4 | +1.5 |
        | Slovenia | 89.1 | 90.4 | +1.3 |
        | Spain | 91.4 | 92.2 | +0.8 |
        | Sweden | 81.3 | 81.4 | +0.2 |
        | Switzerland | 96.7 | 96.8 | +0.1 |
        | Ukraine | 84.3 | 84.4 | +0.1 |
        | United Kingdom | 78.1 | 78.4 | +0.4 |

=== "Oldest and newest"

    | Country | Oldest | Oldest DateIn | Newest | Newest DateIn |
    | :-- | :-- | --: | :-- | --: |
    | Albania | Lanabregas | 1951 | Spitalle Solar Farm | 2027 |
    | Austria | Simmering | 1902 | Barofen Wind Farm | 2027 |
    | Belgium | Beverce Robertville Beverce Robertville Belgium Beverce Robertville Belgium | 1928 | Obourg Floating Solar Farm | 2029 |
    | Bosnia and Herzegovina | Jablanica | 1954 | Dabar | 2027 |
    | Bulgaria | Alexander Stambolyski Site | 1905 | St George Solar Farm | 2026 |
    | Croatia | Miljacka Maljacka | 1906 | Zagocha Geothermal | 2028 |
    | Czechia | Spitalka | 1930 | Komorany | 2027 |
    | Denmark | Orsted | 1920 | Lidso Solar Farm | 2025 |
    | Estonia | Iru Elektrijaam | 1982 | Risti Solar Farm | 2027 |
    | Finland | Alakoski | 1916 | Tervajoensuo Solar Farm | 2026 |
    | France | Cusset | 1899 | Courselles Sur Mer Wind Farm | 2027 |
    | Germany | Hwk | 1851 | Re Rendsburg Eckernforde | 2056 |
    | Greece | Agra | 1954 | Amfilochia Georgios | 2027 |
    | Hungary | Nyiregyhazi | 1897 | Buj | 2026 |
    | Ireland | Ardnacrusha | 1929 | Echelon Dub | 2027 |
    | Italy | Edison Angelo Bertini Paderno | 1898 | Monfalcone | 2026 |
    | Kosovo | Kosovo | 1962 | Gjakova Solar Farm | 2026 |
    | Latvia | Kegums | 1940 | Stelpe Solar Farm | 2025 |
    | Lithuania | Kaunas Algirdas Brazauskas | 1959 | Kelme Enefit Green Wind Farm | 2025 |
    | Luxembourg | Vianden | 1962 | Duelem Wind Park | 2023 |
    | Moldova | Dubasari | 1954 | Moldova Solar | 2024 |
    | Montenegro | Perucica | 1960 | Gvozd Wind Farm | 2026 |
    | Netherlands | Merwedekanaal | 1925 | Eekerpolder Solar Farm | 2026 |
    | North Macedonia | Vrutok | 1957 | Kumanovo Solar | 2024 |
    | Norway | Vik | 1852 | Bergeheia Wind Farm | 2029 |
    | Poland | Wroclaw | 1901 | Gdansk Ccgt | 2029 |
    | Portugal | Ronfe | 1913 | Rio Maior Solar Farm | 2025 |
    | Romania | Bucharest Grozavesti | 1912 | Mintia Deva | 2026 |
    | Serbia | Ovcar Banja Ovcar Banja | 1946 | Ada Solar Farm | 2025 |
    | Slovakia | Ladce | 1936 | Presov Geothermal | 2028 |
    | Slovenia | Fala | 1918 | Kanalski Vrh Solar Farm | 2025 |
    | Spain | Carcavilla | 1902 | Mimosa Hybrid Solar Farm | 2027 |
    | Sweden | Gullspang | 1906 | Fasikan Wind Farm | 2026 |
    | Switzerland | Turgi | 1828 | Kebag Enova Emmenspitz | 2025 |
    | Ukraine | Dnipro | 1932 | Luganskaya | 2024 |
    | United Kingdom | Dolgarrog | 1907 | Hinkley Point C1 | 2029 |

=== "Capacity share by fuel type"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_country/capacity_share.html" title="By country: Capacity share by fuel type" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 1022px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Country | Fueltype | Share old [%] | Share new [%] | Share delta [pp] |
        | :-- | :-- | --: | --: | --: |
        | Albania | Wind | 5.5 | 5.5 | +0 |
        | Albania | Hydro | 76.9 | 76.8 | -0.1 |
        | Albania | Solar | 16.7 | 16.8 | +0.1 |
        | Albania | Battery | 0.9 | 0.9 | +0 |
        | Austria | Wind | 12.9 | 12.3 | -0.6 |
        | Austria | Natural Gas | 18.2 | 17.3 | -0.9 |
        | Austria | Hydro | 56.7 | 57.8 | +1 |
        | Austria | Solar | 2.4 | 3.4 | +0.9 |
        | Austria | Hard Coal | 5.6 | 5.3 | -0.3 |
        | Austria | Lignite | 2 | 1.9 | -0.1 |
        | Austria | Battery | 0.3 | 0.3 | +0 |
        | Austria | Oil | 0.9 | 0.8 | +0 |
        | Austria | Solid Biomass | 0.5 | 0.5 | +0 |
        | Austria | Waste | 0.5 | 0.4 | +0 |
        | Austria | Hydrogen Storage | 0 | 0 | +0 |
        | Belgium | Wind | 18.7 | 18.6 | -0.1 |
        | Belgium | Natural Gas | 29.9 | 29.2 | -0.7 |
        | Belgium | Hydro | 5.8 | 5.7 | -0.1 |
        | Belgium | Solar | 2.1 | 4 | +1.9 |
        | Belgium | Nuclear | 26.3 | 25.7 | -0.6 |
        | Belgium | Hard Coal | 12.1 | 11.8 | -0.3 |
        | Belgium | Battery | 1.2 | 1.2 | +0 |
        | Belgium | Oil | 0.7 | 0.7 | +0 |
        | Belgium | Solid Biomass | 1.7 | 1.6 | +0 |
        | Belgium | Waste | 1.4 | 1.3 | +0 |
        | Belgium | Heat Storage | 0 | 0 | +0 |
        | Bosnia and Herzegovina | Wind | 8 | 7.8 | -0.2 |
        | Bosnia and Herzegovina | Hydro | 42.8 | 42.4 | -0.5 |
        | Bosnia and Herzegovina | Solar | 10.8 | 12.4 | +1.5 |
        | Bosnia and Herzegovina | Lignite | 38.4 | 37.5 | -0.9 |
        | Bulgaria | Wind | 3.3 | 2.7 | -0.5 |
        | Bulgaria | Natural Gas | 4.1 | 3.4 | -0.6 |
        | Bulgaria | Hydro | 15.5 | 13.1 | -2.5 |
        | Bulgaria | Solar | 14.4 | 17.7 | +3.2 |
        | Bulgaria | Nuclear | 20.6 | 17.3 | -3.3 |
        | Bulgaria | Hard Coal | 14.5 | 12.2 | -2.3 |
        | Bulgaria | Lignite | 20.2 | 27.3 | +7.1 |
        | Bulgaria | Battery | 7.5 | 6.3 | -1.2 |
        | Croatia | Wind | 18.8 | 20.7 | +1.9 |
        | Croatia | Natural Gas | 6.8 | 6.4 | -0.4 |
        | Croatia | Hydro | 41.8 | 39.1 | -2.7 |
        | Croatia | Solar | 4.1 | 7.1 | +3.1 |
        | Croatia | Hard Coal | 6.6 | 6.2 | -0.4 |
        | Croatia | Battery | 0 | 0 | +0 |
        | Croatia | Oil | 20.8 | 19.4 | -1.3 |
        | Croatia | Geothermal | 1.1 | 1 | -0.1 |
        | Czechia | Wind | 0.7 | 0.7 | +0 |
        | Czechia | Natural Gas | 9.1 | 8.7 | -0.4 |
        | Czechia | Hydro | 8.7 | 8.3 | -0.4 |
        | Czechia | Solar | 9.4 | 13.3 | +3.9 |
        | Czechia | Nuclear | 19.7 | 18.8 | -0.8 |
        | Czechia | Hard Coal | 7.3 | 7 | -0.3 |
        | Czechia | Lignite | 44.3 | 42.4 | -1.9 |
        | Czechia | Battery | 0.2 | 0.2 | +0 |
        | Czechia | Solid Biomass | 0.5 | 0.5 | +0 |
        | Czechia | Waste | 0.1 | 0.1 | +0 |
        | Denmark | Wind | 28 | 30.4 | +2.4 |
        | Denmark | Natural Gas | 9.2 | 8.6 | -0.6 |
        | Denmark | Solar | 18.4 | 19.2 | +0.8 |
        | Denmark | Hard Coal | 30.6 | 28.7 | -1.8 |
        | Denmark | Battery | 0.9 | 0.9 | -0.1 |
        | Denmark | Oil | 1.2 | 1.1 | -0.1 |
        | Denmark | Solid Biomass | 10.8 | 10.1 | -0.6 |
        | Denmark | Waste | 0.9 | 0.9 | -0.1 |
        | Denmark | Heat Storage | 0 | 0 | +0 |
        | Denmark | Hydrogen Storage | 0 | 0 | +0 |
        | Estonia | Wind | 31.5 | 26.1 | -5.4 |
        | Estonia | Natural Gas | 8.8 | 7.3 | -1.5 |
        | Estonia | Solar | 39.7 | 50.1 | +10.3 |
        | Estonia | Battery | 5.8 | 4.8 | -1 |
        | Estonia | Oil | 10.7 | 8.9 | -1.8 |
        | Estonia | Solid Biomass | 3.4 | 2.8 | -0.6 |
        | Finland | Wind | 30.8 | 30.9 | +0.1 |
        | Finland | Natural Gas | 5.6 | 5.5 | +0 |
        | Finland | Hydro | 10.5 | 10.5 | +0 |
        | Finland | Solar | 2.1 | 2.1 | +0 |
        | Finland | Nuclear | 15.2 | 15.2 | +0 |
        | Finland | Hard Coal | 12.9 | 12.9 | +0 |
        | Finland | Lignite | 0.5 | 0.5 | +0 |
        | Finland | Battery | 1 | 1 | +0 |
        | Finland | Oil | 4.8 | 4.8 | +0 |
        | Finland | Solid Biomass | 12.3 | 12.3 | +0 |
        | Finland | Waste | 0.4 | 0.4 | +0 |
        | Finland | Biogas | 1.8 | 1.8 | +0 |
        | Finland | Heat Storage | 2.1 | 2.1 | +0 |
        | Finland | Hydrogen Storage | 0 | 0 | +0 |
        | France | Wind | 17.8 | 17.9 | +0.1 |
        | France | Natural Gas | 6 | 6 | +0 |
        | France | Hydro | 14.6 | 14.6 | -0.1 |
        | France | Solar | 8.4 | 8.7 | +0.3 |
        | France | Nuclear | 45.5 | 45.3 | -0.3 |
        | France | Hard Coal | 4.6 | 4.6 | +0 |
        | France | Lignite | 0.8 | 0.8 | +0 |
        | France | Battery | 0.4 | 0.4 | +0 |
        | France | Oil | 1.4 | 1.4 | +0 |
        | France | Solid Biomass | 0.3 | 0.3 | +0 |
        | France | Waste | 0.1 | 0.1 | +0 |
        | France | Biogas | 0 | 0 | +0 |
        | France | Heat Storage | 0 | 0 | +0 |
        | France | Geothermal | 0 | 0 | +0 |
        | France | Hydrogen Storage | 0 | 0 | +0 |
        | Germany | Wind | 12 | 33.7 | +21.8 |
        | Germany | Natural Gas | 17.1 | 12.6 | -4.5 |
        | Germany | Hydro | 4.6 | 3.1 | -1.4 |
        | Germany | Solar | 21.8 | 18.6 | -3.3 |
        | Germany | Nuclear | 10.2 | 6.8 | -3.3 |
        | Germany | Hard Coal | 14.8 | 10 | -4.8 |
        | Germany | Lignite | 7.8 | 5.2 | -2.6 |
        | Germany | Battery | 5.5 | 4 | -1.5 |
        | Germany | Oil | 3 | 2.4 | -0.6 |
        | Germany | Solid Biomass | 0.4 | 0.3 | -0.1 |
        | Germany | Waste | 1.2 | 0.9 | -0.4 |
        | Germany | Biogas | 1.2 | 2 | +0.8 |
        | Germany | Heat Storage | 0 | 0 | +0 |
        | Germany | Other | 0.4 | 0.3 | -0.1 |
        | Germany | Geothermal | 0 | 0 | +0 |
        | Germany | Hydrogen Storage | 0 | 0 | +0 |
        | Greece | Wind | 17.7 | 17.2 | -0.5 |
        | Greece | Natural Gas | 22.6 | 21.4 | -1.2 |
        | Greece | Hydro | 11 | 10.4 | -0.6 |
        | Greece | Solar | 28.2 | 31.5 | +3.3 |
        | Greece | Lignite | 16.3 | 15.5 | -0.9 |
        | Greece | Battery | 0.2 | 0.2 | +0 |
        | Greece | Oil | 3.9 | 3.7 | -0.2 |
        | Greece | Waste | 0.1 | 0.1 | +0 |
        | Greece | Hydrogen Storage | 0 | 0 | +0 |
        | Hungary | Wind | 2.9 | 2.6 | -0.3 |
        | Hungary | Natural Gas | 27.9 | 25.4 | -2.6 |
        | Hungary | Hydro | 0.4 | 0.4 | +0 |
        | Hungary | Solar | 26.4 | 33.2 | +6.7 |
        | Hungary | Nuclear | 18.2 | 16.5 | -1.7 |
        | Hungary | Hard Coal | 4.4 | 4 | -0.4 |
        | Hungary | Lignite | 11 | 10 | -1 |
        | Hungary | Battery | 1.7 | 1.6 | -0.2 |
        | Hungary | Oil | 4 | 3.7 | -0.4 |
        | Hungary | Solid Biomass | 2.7 | 2.5 | -0.2 |
        | Hungary | Waste | 0.2 | 0.2 | +0 |
        | Hungary | Geothermal | 0 | 0 | +0 |
        | Hungary | Hydrogen Storage | 0 | 0 | +0 |
        | Ireland | Wind | 28.2 | 27.9 | -0.2 |
        | Ireland | Natural Gas | 33.7 | 34.6 | +0.9 |
        | Ireland | Hydro | 3.3 | 3.2 | -0.1 |
        | Ireland | Solar | 9.2 | 9 | -0.2 |
        | Ireland | Hard Coal | 5.4 | 5.3 | -0.1 |
        | Ireland | Lignite | 0.8 | 0.8 | +0 |
        | Ireland | Battery | 4.9 | 4.8 | -0.1 |
        | Ireland | Oil | 13.9 | 13.6 | -0.2 |
        | Ireland | Solid Biomass | 0.2 | 0.2 | +0 |
        | Ireland | Waste | 0.5 | 0.5 | +0 |
        | Italy | Wind | 9.1 | 9.5 | +0.3 |
        | Italy | Natural Gas | 48 | 47.7 | -0.3 |
        | Italy | Hydro | 20.8 | 20.8 | +0 |
        | Italy | Solar | 5.8 | 5.9 | +0.1 |
        | Italy | Nuclear | 1.3 | 1.3 | +0 |
        | Italy | Hard Coal | 10 | 10 | -0.1 |
        | Italy | Battery | 1.5 | 1.5 | +0 |
        | Italy | Oil | 2.1 | 2.1 | +0 |
        | Italy | Solid Biomass | 0.2 | 0.2 | +0 |
        | Italy | Waste | 0.3 | 0.3 | +0 |
        | Italy | Biogas | 0 | 0 | +0 |
        | Italy | Heat Storage | 0 | 0 | +0 |
        | Italy | Geothermal | 0.8 | 0.8 | +0 |
        | Italy | Hydrogen Storage | 0 | 0 | +0 |
        | Kosovo | Wind | 7.4 | 7.3 | -0.1 |
        | Kosovo | Hydro | 3.6 | 3.5 | +0 |
        | Kosovo | Solar | 8.4 | 9 | +0.7 |
        | Kosovo | Lignite | 80.7 | 80.1 | -0.6 |
        | Latvia | Wind | 2.9 | 2.7 | -0.2 |
        | Latvia | Natural Gas | 26.9 | 29.6 | +2.8 |
        | Latvia | Hydro | 44.4 | 41.9 | -2.5 |
        | Latvia | Solar | 21.9 | 22.1 | +0.2 |
        | Latvia | Battery | 3.2 | 3 | -0.2 |
        | Latvia | Solid Biomass | 0.7 | 0.6 | +0 |
        | Lithuania | Wind | 30.9 | 30.6 | -0.3 |
        | Lithuania | Natural Gas | 16.6 | 16.5 | -0.2 |
        | Lithuania | Hydro | 11 | 10.9 | -0.1 |
        | Lithuania | Solar | 8.7 | 9.5 | +0.8 |
        | Lithuania | Nuclear | 28.7 | 28.4 | -0.3 |
        | Lithuania | Battery | 2.5 | 2.5 | +0 |
        | Lithuania | Solid Biomass | 0.9 | 0.9 | +0 |
        | Lithuania | Waste | 0.7 | 0.7 | +0 |
        | Luxembourg | Wind | 8.5 | 8.5 | +0 |
        | Luxembourg | Hydro | 84.1 | 83.8 | -0.3 |
        | Luxembourg | Solar | 6.2 | 6.5 | +0.3 |
        | Luxembourg | Solid Biomass | 0.2 | 0.2 | +0 |
        | Luxembourg | Waste | 1 | 1 | +0 |
        | Moldova | Natural Gas | 69.9 | 69.4 | -0.5 |
        | Moldova | Hydro | 1.2 | 1.2 | +0 |
        | Moldova | Solar | 2.4 | 3.1 | +0.7 |
        | Moldova | Hard Coal | 26.5 | 26.3 | -0.2 |
        | Montenegro | Wind | 16.1 | 16 | +0 |
        | Montenegro | Hydro | 60.8 | 60.7 | -0.2 |
        | Montenegro | Solar | 2.2 | 2.4 | +0.3 |
        | Montenegro | Lignite | 20.9 | 20.9 | -0.1 |
        | Netherlands | Wind | 23.6 | 26.5 | +2.9 |
        | Netherlands | Natural Gas | 39.2 | 36.7 | -2.5 |
        | Netherlands | Solar | 14 | 15.2 | +1.2 |
        | Netherlands | Nuclear | 1.3 | 1.2 | -0.1 |
        | Netherlands | Hard Coal | 17.2 | 16 | -1.2 |
        | Netherlands | Battery | 0.9 | 0.8 | -0.1 |
        | Netherlands | Oil | 0.2 | 0.2 | +0 |
        | Netherlands | Solid Biomass | 1.6 | 1.5 | -0.1 |
        | Netherlands | Waste | 1.5 | 1.4 | -0.1 |
        | Netherlands | Biogas | 0.4 | 0.3 | +0 |
        | Netherlands | Heat Storage | 0 | 0 | +0 |
        | Netherlands | Hydrogen Storage | 0 | 0 | +0 |
        | Netherlands | Mechanical Storage | 0 | 0 | +0 |
        | North Macedonia | Wind | 19.4 | 18.4 | -1 |
        | North Macedonia | Natural Gas | 7.8 | 7.4 | -0.4 |
        | North Macedonia | Hydro | 20.6 | 19.5 | -1.1 |
        | North Macedonia | Solar | 14.5 | 18.8 | +4.4 |
        | North Macedonia | Lignite | 29.3 | 27.8 | -1.5 |
        | North Macedonia | Battery | 1.4 | 1.3 | -0.1 |
        | North Macedonia | Oil | 7 | 6.7 | -0.4 |
        | Norway | Wind | 15.9 | 15.4 | -0.5 |
        | Norway | Natural Gas | 2.4 | 2.3 | -0.1 |
        | Norway | Hydro | 81.5 | 82.1 | +0.6 |
        | Norway | Solar | 0.1 | 0.1 | +0 |
        | Norway | Battery | 0 | 0 | +0 |
        | Norway | Hydrogen Storage | 0 | 0 | +0 |
        | Poland | Wind | 15.4 | 15.4 | +0 |
        | Poland | Natural Gas | 13.4 | 12.9 | -0.4 |
        | Poland | Hydro | 3.4 | 3.3 | -0.1 |
        | Poland | Solar | 13.9 | 13.5 | -0.4 |
        | Poland | Hard Coal | 36.8 | 36.5 | -0.2 |
        | Poland | Lignite | 15.6 | 16.9 | +1.3 |
        | Poland | Battery | 0.4 | 0.3 | +0 |
        | Poland | Solid Biomass | 1.2 | 1.1 | +0 |
        | Poland | Waste | 0 | 0 | +0 |
        | Portugal | Wind | 25.3 | 26.3 | +1 |
        | Portugal | Natural Gas | 16.1 | 15.8 | -0.3 |
        | Portugal | Hydro | 31.8 | 31.5 | -0.3 |
        | Portugal | Solar | 15 | 14.8 | -0.2 |
        | Portugal | Hard Coal | 7.7 | 7.5 | -0.1 |
        | Portugal | Battery | 0.2 | 0.2 | +0 |
        | Portugal | Oil | 1.8 | 1.7 | +0 |
        | Portugal | Solid Biomass | 1.7 | 1.6 | +0 |
        | Portugal | Waste | 0.4 | 0.4 | +0 |
        | Portugal | Geothermal | 0.1 | 0.1 | +0 |
        | Portugal | Hydrogen Storage | 0 | 0 | +0 |
        | Portugal | Mechanical Storage | 0 | 0 | +0 |
        | Romania | Wind | 18.9 | 17.8 | -1 |
        | Romania | Natural Gas | 17.7 | 16.7 | -1 |
        | Romania | Hydro | 21.6 | 22.1 | +0.5 |
        | Romania | Solar | 10.5 | 13.8 | +3.3 |
        | Romania | Nuclear | 4.3 | 4 | -0.2 |
        | Romania | Hard Coal | 5.2 | 4.9 | -0.3 |
        | Romania | Lignite | 18.5 | 17.5 | -1 |
        | Romania | Battery | 2.6 | 2.4 | -0.1 |
        | Romania | Oil | 0.6 | 0.6 | +0 |
        | Romania | Solid Biomass | 0.1 | 0.1 | +0 |
        | Serbia | Wind | 7.8 | 6.7 | -1.1 |
        | Serbia | Natural Gas | 6.9 | 6 | -1 |
        | Serbia | Hydro | 41.5 | 36.7 | -4.9 |
        | Serbia | Solar | 2.2 | 2 | -0.2 |
        | Serbia | Lignite | 41.1 | 48.3 | +7.2 |
        | Serbia | Solid Biomass | 0.1 | 0.1 | +0 |
        | Serbia | Waste | 0.4 | 0.3 | +0 |
        | Slovakia | Natural Gas | 11.5 | 10.8 | -0.7 |
        | Slovakia | Hydro | 26.3 | 24.7 | -1.6 |
        | Slovakia | Solar | 3.2 | 9.1 | +5.9 |
        | Slovakia | Nuclear | 42.5 | 39.9 | -2.6 |
        | Slovakia | Hard Coal | 10.6 | 9.9 | -0.6 |
        | Slovakia | Lignite | 5.9 | 5.5 | -0.4 |
        | Slovakia | Battery | 0.1 | 0.1 | +0 |
        | Slovakia | Geothermal | 0.1 | 0.1 | +0 |
        | Slovenia | Natural Gas | 14.1 | 14 | -0.1 |
        | Slovenia | Hydro | 28.5 | 28.2 | -0.2 |
        | Slovenia | Solar | 1.3 | 2.2 | +0.9 |
        | Slovenia | Nuclear | 17 | 16.9 | -0.1 |
        | Slovenia | Hard Coal | 2.9 | 2.9 | +0 |
        | Slovenia | Lignite | 34.7 | 34.4 | -0.3 |
        | Slovenia | Battery | 1.5 | 1.5 | +0 |
        | Spain | Wind | 21.6 | 22.1 | +0.5 |
        | Spain | Natural Gas | 19.5 | 18.7 | -0.8 |
        | Spain | Hydro | 12.8 | 12.3 | -0.5 |
        | Spain | Solar | 28.1 | 29.6 | +1.5 |
        | Spain | Nuclear | 6 | 5.7 | -0.2 |
        | Spain | Hard Coal | 7.9 | 7.5 | -0.3 |
        | Spain | Lignite | 1.9 | 1.8 | -0.1 |
        | Spain | Battery | 0.2 | 0.2 | +0 |
        | Spain | Oil | 0.8 | 0.7 | +0 |
        | Spain | Solid Biomass | 0.4 | 0.4 | +0 |
        | Spain | Waste | 0.2 | 0.2 | +0 |
        | Spain | Heat Storage | 0.7 | 0.7 | +0 |
        | Spain | Hydrogen Storage | 0 | 0 | +0 |
        | Sweden | Wind | 31.1 | 34.4 | +3.3 |
        | Sweden | Natural Gas | 1.5 | 1.5 | -0.1 |
        | Sweden | Hydro | 29.5 | 28.1 | -1.4 |
        | Sweden | Solar | 1.3 | 1.3 | -0.1 |
        | Sweden | Nuclear | 22.1 | 21 | -1 |
        | Sweden | Hard Coal | 0.6 | 0.5 | +0 |
        | Sweden | Battery | 1.5 | 1.4 | -0.1 |
        | Sweden | Oil | 5.4 | 5.1 | -0.3 |
        | Sweden | Solid Biomass | 6.1 | 5.9 | -0.3 |
        | Sweden | Waste | 0.9 | 0.8 | +0 |
        | Switzerland | Wind | 0.2 | 0.2 | +0 |
        | Switzerland | Natural Gas | 0.4 | 0.4 | +0 |
        | Switzerland | Hydro | 81.3 | 81.6 | +0.3 |
        | Switzerland | Solar | 0.4 | 0.4 | +0 |
        | Switzerland | Nuclear | 15.9 | 15.6 | -0.3 |
        | Switzerland | Battery | 0.3 | 0.3 | +0 |
        | Switzerland | Solid Biomass | 0.3 | 0.3 | +0 |
        | Switzerland | Waste | 1 | 1 | +0 |
        | Switzerland | Biogas | 0 | 0 | +0 |
        | Switzerland | Hydrogen Storage | 0 | 0 | +0 |
        | Switzerland | Mechanical Storage | 0 | 0 | +0 |
        | Ukraine | Wind | 1 | 1 | +0 |
        | Ukraine | Natural Gas | 6.3 | 7.5 | +1.1 |
        | Ukraine | Hydro | 11.4 | 11.3 | -0.1 |
        | Ukraine | Solar | 9.9 | 9.7 | -0.1 |
        | Ukraine | Nuclear | 30.6 | 30.2 | -0.4 |
        | Ukraine | Hard Coal | 40.5 | 40 | -0.5 |
        | Ukraine | Battery | 0.3 | 0.3 | +0 |
        | United Kingdom | Wind | 22.5 | 23.7 | +1.2 |
        | United Kingdom | Natural Gas | 24.2 | 23.8 | -0.4 |
        | United Kingdom | Hydro | 2.9 | 2.8 | +0 |
        | United Kingdom | Solar | 8.3 | 8.2 | -0.1 |
        | United Kingdom | Nuclear | 11.6 | 11.4 | -0.2 |
        | United Kingdom | Hard Coal | 21.8 | 21.4 | -0.3 |
        | United Kingdom | Battery | 4.5 | 4.4 | -0.1 |
        | United Kingdom | Oil | 0.4 | 0.4 | +0 |
        | United Kingdom | Solid Biomass | 2.5 | 2.5 | +0 |
        | United Kingdom | Waste | 1.3 | 1.2 | +0 |
        | United Kingdom | Biogas | 0.1 | 0.1 | +0 |
        | United Kingdom | Geothermal | 0 | 0 | +0 |
        | United Kingdom | Hydrogen Storage | 0 | 0 | +0 |
        | United Kingdom | Mechanical Storage | 0 | 0 | +0 |

=== "Entry share by fuel type"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_country/entry_share.html" title="By country: Entry share by fuel type" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 1022px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Country | Fueltype | Share old [%] | Share new [%] | Share delta [pp] |
        | :-- | :-- | --: | --: | --: |
        | Albania | Solar | 33.3 | 34.7 | +1.4 |
        | Albania | Wind | 2.1 | 2 | +0 |
        | Albania | Hydro | 62.5 | 61.2 | -1.3 |
        | Albania | Battery | 2.1 | 2 | +0 |
        | Austria | Solar | 20.4 | 34.5 | +14.1 |
        | Austria | Wind | 23.6 | 19.3 | -4.3 |
        | Austria | Natural Gas | 4.3 | 3.5 | -0.8 |
        | Austria | Hydro | 44 | 36.3 | -7.6 |
        | Austria | Battery | 1.5 | 1.2 | -0.3 |
        | Austria | Oil | 0.5 | 0.4 | -0.1 |
        | Austria | Solid Biomass | 1.8 | 1.4 | -0.3 |
        | Austria | Waste | 1.3 | 1 | -0.2 |
        | Austria | Hard Coal | 1.3 | 1 | -0.2 |
        | Austria | Lignite | 0.5 | 0.4 | -0.1 |
        | Austria | Hydrogen Storage | 1 | 0.8 | -0.2 |
        | Belgium | Solar | 23.4 | 43.4 | +20.1 |
        | Belgium | Wind | 37.7 | 28.1 | -9.6 |
        | Belgium | Natural Gas | 11.1 | 8.1 | -3 |
        | Belgium | Hydro | 5.3 | 3.9 | -1.4 |
        | Belgium | Battery | 5.7 | 4.2 | -1.5 |
        | Belgium | Oil | 3.3 | 2.4 | -0.9 |
        | Belgium | Solid Biomass | 2 | 1.5 | -0.6 |
        | Belgium | Waste | 4.1 | 3 | -1.1 |
        | Belgium | Hard Coal | 3.7 | 2.7 | -1 |
        | Belgium | Nuclear | 3.3 | 2.4 | -0.9 |
        | Belgium | Heat Storage | 0.4 | 0.3 | -0.1 |
        | Bosnia and Herzegovina | Solar | 54.1 | 66.7 | +12.6 |
        | Bosnia and Herzegovina | Wind | 11.5 | 8 | -3.4 |
        | Bosnia and Herzegovina | Hydro | 26.2 | 19.5 | -6.7 |
        | Bosnia and Herzegovina | Lignite | 8.2 | 5.7 | -2.4 |
        | Bulgaria | Solar | 66.3 | 79.1 | +12.8 |
        | Bulgaria | Wind | 6.6 | 4 | -2.6 |
        | Bulgaria | Natural Gas | 2.3 | 1.4 | -0.9 |
        | Bulgaria | Hydro | 11.6 | 7.1 | -4.6 |
        | Bulgaria | Battery | 5.8 | 3.5 | -2.3 |
        | Bulgaria | Hard Coal | 2.3 | 1.4 | -0.9 |
        | Bulgaria | Nuclear | 2.3 | 1.4 | -0.9 |
        | Bulgaria | Lignite | 2.7 | 2.1 | -0.6 |
        | Croatia | Solar | 25.3 | 48.3 | +23 |
        | Croatia | Wind | 31.6 | 23.3 | -8.3 |
        | Croatia | Natural Gas | 3.8 | 2.5 | -1.3 |
        | Croatia | Hydro | 27.8 | 18.3 | -9.5 |
        | Croatia | Battery | 1.3 | 0.8 | -0.4 |
        | Croatia | Oil | 5.1 | 3.3 | -1.7 |
        | Croatia | Hard Coal | 1.3 | 0.8 | -0.4 |
        | Croatia | Geothermal | 3.8 | 2.5 | -1.3 |
        | Czechia | Solar | 82.4 | 88 | +5.6 |
        | Czechia | Wind | 1.5 | 1 | -0.5 |
        | Czechia | Natural Gas | 2.6 | 1.8 | -0.8 |
        | Czechia | Hydro | 1.9 | 1.3 | -0.6 |
        | Czechia | Battery | 1.7 | 1.2 | -0.5 |
        | Czechia | Solid Biomass | 0.4 | 0.3 | -0.1 |
        | Czechia | Waste | 0.4 | 0.3 | -0.1 |
        | Czechia | Hard Coal | 2.4 | 1.6 | -0.7 |
        | Czechia | Nuclear | 1.3 | 0.9 | -0.4 |
        | Czechia | Lignite | 5.4 | 3.7 | -1.7 |
        | Denmark | Solar | 41.7 | 54.8 | +13.1 |
        | Denmark | Wind | 38.6 | 30.3 | -8.3 |
        | Denmark | Natural Gas | 4.1 | 3.1 | -1 |
        | Denmark | Battery | 4.8 | 3.7 | -1.2 |
        | Denmark | Oil | 0.7 | 0.5 | -0.2 |
        | Denmark | Solid Biomass | 3.4 | 2.6 | -0.8 |
        | Denmark | Waste | 1.4 | 1 | -0.3 |
        | Denmark | Hard Coal | 3.4 | 2.6 | -0.8 |
        | Denmark | Hydrogen Storage | 1 | 0.8 | -0.3 |
        | Denmark | Heat Storage | 0.7 | 0.5 | -0.2 |
        | Estonia | Solar | 67.1 | 85.9 | +18.8 |
        | Estonia | Wind | 22.4 | 9.6 | -12.8 |
        | Estonia | Natural Gas | 1.3 | 0.6 | -0.8 |
        | Estonia | Battery | 3.9 | 1.7 | -2.3 |
        | Estonia | Oil | 1.3 | 0.6 | -0.8 |
        | Estonia | Solid Biomass | 3.9 | 1.7 | -2.3 |
        | Finland | Solar | 7.5 | 7.5 | +0 |
        | Finland | Wind | 32.5 | 32.9 | +0.3 |
        | Finland | Biogas | 3.8 | 3.8 | +0 |
        | Finland | Natural Gas | 3.1 | 3.1 | +0 |
        | Finland | Hydro | 25.9 | 25.8 | -0.1 |
        | Finland | Battery | 4 | 4 | +0 |
        | Finland | Oil | 4.2 | 4.2 | +0 |
        | Finland | Solid Biomass | 8 | 8 | +0 |
        | Finland | Waste | 0.7 | 0.7 | +0 |
        | Finland | Hard Coal | 3.8 | 3.8 | +0 |
        | Finland | Nuclear | 1.2 | 1.2 | +0 |
        | Finland | Lignite | 0.2 | 0.2 | +0 |
        | Finland | Hydrogen Storage | 0.5 | 0.5 | +0 |
        | Finland | Heat Storage | 4.5 | 4.5 | +0 |
        | France | Solar | 49.8 | 51.3 | +1.5 |
        | France | Wind | 36 | 35.1 | -0.9 |
        | France | Biogas | 0 | 0 | +0 |
        | France | Natural Gas | 1.5 | 1.4 | -0.1 |
        | France | Hydro | 7.6 | 7.3 | -0.3 |
        | France | Battery | 1.1 | 1 | +0 |
        | France | Oil | 0.6 | 0.6 | +0 |
        | France | Solid Biomass | 0.3 | 0.3 | +0 |
        | France | Waste | 0.2 | 0.2 | +0 |
        | France | Hard Coal | 0.4 | 0.4 | +0 |
        | France | Nuclear | 2.3 | 2.2 | -0.1 |
        | France | Lignite | 0.1 | 0.1 | +0 |
        | France | Hydrogen Storage | 0.1 | 0.1 | +0 |
        | France | Geothermal | 0 | 0 | +0 |
        | France | Heat Storage | 0 | 0 | +0 |
        | Germany | Solar | 75.3 | 67 | -8.3 |
        | Germany | Wind | 7.2 | 15 | +7.7 |
        | Germany | Biogas | 5 | 7.5 | +2.5 |
        | Germany | Natural Gas | 5.6 | 5 | -0.6 |
        | Germany | Hydro | 1.6 | 1.3 | -0.4 |
        | Germany | Battery | 2.8 | 2 | -0.8 |
        | Germany | Oil | 1.7 | 1.6 | +0 |
        | Germany | Solid Biomass | 0.4 | 0.3 | +0 |
        | Germany | Waste | 0.2 | 0.1 | -0.1 |
        | Germany | Hard Coal | 0.1 | 0 | +0 |
        | Germany | Nuclear | 0 | 0 | +0 |
        | Germany | Other | 0.1 | 0.1 | +0 |
        | Germany | Lignite | 0 | 0 | +0 |
        | Germany | Hydrogen Storage | 0 | 0 | +0 |
        | Germany | Geothermal | 0 | 0 | +0 |
        | Germany | Heat Storage | 0 | 0 | +0 |
        | Greece | Solar | 76.5 | 80.6 | +4.1 |
        | Greece | Wind | 20.4 | 16.8 | -3.5 |
        | Greece | Natural Gas | 0.7 | 0.6 | -0.1 |
        | Greece | Hydro | 1.1 | 0.9 | -0.2 |
        | Greece | Battery | 0.1 | 0.1 | +0 |
        | Greece | Oil | 0.4 | 0.3 | -0.1 |
        | Greece | Waste | 0.1 | 0 | +0 |
        | Greece | Lignite | 0.5 | 0.4 | -0.1 |
        | Greece | Hydrogen Storage | 0.2 | 0.2 | +0 |
        | Hungary | Solar | 87 | 90.4 | +3.4 |
        | Hungary | Wind | 2.2 | 1.6 | -0.6 |
        | Hungary | Natural Gas | 3.5 | 2.6 | -0.9 |
        | Hungary | Hydro | 0.9 | 0.6 | -0.2 |
        | Hungary | Battery | 1.7 | 1.3 | -0.5 |
        | Hungary | Oil | 0.6 | 0.5 | -0.2 |
        | Hungary | Solid Biomass | 0.9 | 0.6 | -0.2 |
        | Hungary | Waste | 0.2 | 0.2 | -0.1 |
        | Hungary | Hard Coal | 0.9 | 0.6 | -0.2 |
        | Hungary | Nuclear | 0.9 | 0.6 | -0.2 |
        | Hungary | Lignite | 0.6 | 0.5 | -0.2 |
        | Hungary | Hydrogen Storage | 0.4 | 0.3 | -0.1 |
        | Hungary | Geothermal | 0.2 | 0.2 | -0.1 |
        | Ireland | Solar | 17.7 | 17.4 | -0.2 |
        | Ireland | Wind | 57.3 | 57.4 | +0.1 |
        | Ireland | Natural Gas | 8.2 | 8.5 | +0.3 |
        | Ireland | Hydro | 2.6 | 2.6 | +0 |
        | Ireland | Battery | 9.1 | 8.9 | -0.1 |
        | Ireland | Oil | 3 | 3 | +0 |
        | Ireland | Solid Biomass | 0.4 | 0.4 | +0 |
        | Ireland | Waste | 0.9 | 0.9 | +0 |
        | Ireland | Hard Coal | 0.4 | 0.4 | +0 |
        | Ireland | Lignite | 0.4 | 0.4 | +0 |
        | Italy | Solar | 22.7 | 22.7 | +0 |
        | Italy | Wind | 24.5 | 24.9 | +0.4 |
        | Italy | Biogas | 0.1 | 0.1 | +0 |
        | Italy | Natural Gas | 9.8 | 9.7 | -0.1 |
        | Italy | Hydro | 31.8 | 31.7 | -0.1 |
        | Italy | Battery | 4.4 | 4.3 | -0.1 |
        | Italy | Oil | 1 | 1 | +0 |
        | Italy | Solid Biomass | 0.7 | 0.7 | +0 |
        | Italy | Waste | 0.6 | 0.6 | +0 |
        | Italy | Hard Coal | 1.3 | 1.3 | +0 |
        | Italy | Nuclear | 0.3 | 0.3 | +0 |
        | Italy | Hydrogen Storage | 0.4 | 0.4 | +0 |
        | Italy | Geothermal | 2 | 2 | +0 |
        | Italy | Heat Storage | 0.2 | 0.2 | +0 |
        | Kosovo | Solar | 28.6 | 44.4 | +15.9 |
        | Kosovo | Wind | 28.6 | 22.2 | -6.3 |
        | Kosovo | Hydro | 28.6 | 22.2 | -6.3 |
        | Kosovo | Lignite | 14.3 | 11.1 | -3.2 |
        | Latvia | Solar | 80.2 | 81.3 | +1.1 |
        | Latvia | Wind | 3.7 | 3.3 | -0.4 |
        | Latvia | Natural Gas | 2.5 | 3.3 | +0.8 |
        | Latvia | Hydro | 3.7 | 3.3 | -0.4 |
        | Latvia | Battery | 8.6 | 7.7 | -0.9 |
        | Latvia | Solid Biomass | 1.2 | 1.1 | -0.1 |
        | Lithuania | Solar | 69.8 | 73.3 | +3.5 |
        | Lithuania | Wind | 18.6 | 16.4 | -2.2 |
        | Lithuania | Natural Gas | 3.1 | 2.7 | -0.4 |
        | Lithuania | Hydro | 1.6 | 1.4 | -0.2 |
        | Lithuania | Battery | 2.3 | 2.1 | -0.3 |
        | Lithuania | Solid Biomass | 0.8 | 0.7 | -0.1 |
        | Lithuania | Waste | 2.3 | 2.1 | -0.3 |
        | Lithuania | Nuclear | 1.6 | 1.4 | -0.2 |
        | Luxembourg | Solar | 65.6 | 66.7 | +1 |
        | Luxembourg | Wind | 25 | 24.2 | -0.8 |
        | Luxembourg | Hydro | 3.1 | 3 | -0.1 |
        | Luxembourg | Solid Biomass | 3.1 | 3 | -0.1 |
        | Luxembourg | Waste | 3.1 | 3 | -0.1 |
        | Moldova | Solar | 85 | 87.5 | +2.5 |
        | Moldova | Natural Gas | 7.5 | 6.2 | -1.2 |
        | Moldova | Hydro | 5 | 4.2 | -0.8 |
        | Moldova | Hard Coal | 2.5 | 2.1 | -0.4 |
        | Montenegro | Solar | 30 | 41.7 | +11.7 |
        | Montenegro | Wind | 30 | 25 | -5 |
        | Montenegro | Hydro | 30 | 25 | -5 |
        | Montenegro | Lignite | 10 | 8.3 | -1.7 |
        | Netherlands | Solar | 60.9 | 66.5 | +5.6 |
        | Netherlands | Wind | 23.1 | 20 | -3 |
        | Netherlands | Biogas | 0.4 | 0.4 | -0.1 |
        | Netherlands | Natural Gas | 6.4 | 5.4 | -1 |
        | Netherlands | Battery | 4.5 | 3.7 | -0.8 |
        | Netherlands | Oil | 0.1 | 0.1 | +0 |
        | Netherlands | Solid Biomass | 1.2 | 1 | -0.2 |
        | Netherlands | Waste | 1.5 | 1.2 | -0.3 |
        | Netherlands | Hard Coal | 1 | 0.9 | -0.2 |
        | Netherlands | Nuclear | 0.3 | 0.2 | -0.1 |
        | Netherlands | Hydrogen Storage | 0.3 | 0.2 | -0.1 |
        | Netherlands | Heat Storage | 0.1 | 0.1 | +0 |
        | Netherlands | Mechanical Storage | 0.1 | 0.1 | +0 |
        | North Macedonia | Solar | 69.1 | 78.1 | +9 |
        | North Macedonia | Wind | 7.4 | 5.2 | -2.1 |
        | North Macedonia | Natural Gas | 1.5 | 1 | -0.4 |
        | North Macedonia | Hydro | 14.7 | 10.4 | -4.3 |
        | North Macedonia | Battery | 2.9 | 2.1 | -0.9 |
        | North Macedonia | Oil | 1.5 | 1 | -0.4 |
        | North Macedonia | Lignite | 2.9 | 2.1 | -0.9 |
        | Norway | Solar | 0.8 | 0.8 | +0.1 |
        | Norway | Wind | 5.5 | 5.5 | +0 |
        | Norway | Natural Gas | 0.5 | 0.5 | +0 |
        | Norway | Hydro | 92.3 | 92.2 | -0.1 |
        | Norway | Battery | 0.6 | 0.7 | +0.1 |
        | Norway | Hydrogen Storage | 0.4 | 0.4 | +0 |
        | Poland | Solar | 86.7 | 86.3 | -0.4 |
        | Poland | Wind | 8.1 | 8.4 | +0.3 |
        | Poland | Natural Gas | 1.2 | 1.2 | +0 |
        | Poland | Hydro | 1.3 | 1.3 | +0 |
        | Poland | Battery | 0.3 | 0.3 | +0 |
        | Poland | Solid Biomass | 0.3 | 0.3 | +0 |
        | Poland | Waste | 0 | 0 | +0 |
        | Poland | Hard Coal | 1.8 | 1.9 | +0.1 |
        | Poland | Lignite | 0.3 | 0.3 | +0 |
        | Portugal | Solar | 27.9 | 27.1 | -0.8 |
        | Portugal | Wind | 39.8 | 42.1 | +2.3 |
        | Portugal | Natural Gas | 2 | 1.9 | -0.1 |
        | Portugal | Hydro | 21.7 | 20.7 | -1 |
        | Portugal | Battery | 1.3 | 1.2 | -0.1 |
        | Portugal | Oil | 0.8 | 0.8 | +0 |
        | Portugal | Solid Biomass | 2.6 | 2.5 | -0.1 |
        | Portugal | Waste | 2.4 | 2.3 | -0.1 |
        | Portugal | Hard Coal | 0.5 | 0.5 | +0 |
        | Portugal | Hydrogen Storage | 0.3 | 0.3 | +0 |
        | Portugal | Geothermal | 0.3 | 0.3 | +0 |
        | Portugal | Mechanical Storage | 0.3 | 0.3 | +0 |
        | Romania | Solar | 45.2 | 63 | +17.9 |
        | Romania | Wind | 12.3 | 8.3 | -4.1 |
        | Romania | Natural Gas | 4.3 | 2.9 | -1.4 |
        | Romania | Hydro | 28.7 | 19.4 | -9.3 |
        | Romania | Battery | 3.9 | 2.6 | -1.3 |
        | Romania | Oil | 0.5 | 0.3 | -0.2 |
        | Romania | Solid Biomass | 0.2 | 0.2 | -0.1 |
        | Romania | Hard Coal | 1.2 | 0.8 | -0.4 |
        | Romania | Nuclear | 0.5 | 0.3 | -0.2 |
        | Romania | Lignite | 3.1 | 2.1 | -1 |
        | Serbia | Solar | 46 | 47.1 | +1.1 |
        | Serbia | Wind | 14.3 | 12.9 | -1.4 |
        | Serbia | Natural Gas | 4.8 | 4.3 | -0.5 |
        | Serbia | Hydro | 23.8 | 24.3 | +0.5 |
        | Serbia | Solid Biomass | 3.2 | 2.9 | -0.3 |
        | Serbia | Waste | 1.6 | 1.4 | -0.2 |
        | Serbia | Lignite | 6.3 | 7.1 | +0.8 |
        | Slovakia | Solar | 54.9 | 75.5 | +20.6 |
        | Slovakia | Natural Gas | 6.6 | 3.4 | -3.1 |
        | Slovakia | Hydro | 24.6 | 13.7 | -10.9 |
        | Slovakia | Battery | 0.8 | 0.4 | -0.4 |
        | Slovakia | Hard Coal | 2.5 | 1.3 | -1.2 |
        | Slovakia | Nuclear | 7.4 | 3.9 | -3.5 |
        | Slovakia | Lignite | 2.5 | 1.3 | -1.2 |
        | Slovakia | Geothermal | 0.8 | 0.4 | -0.4 |
        | Slovenia | Solar | 40.6 | 47.9 | +7.3 |
        | Slovenia | Natural Gas | 4.7 | 4.1 | -0.6 |
        | Slovenia | Hydro | 39.1 | 34.2 | -4.8 |
        | Slovenia | Battery | 9.4 | 8.2 | -1.2 |
        | Slovenia | Hard Coal | 1.6 | 1.4 | -0.2 |
        | Slovenia | Nuclear | 1.6 | 1.4 | -0.2 |
        | Slovenia | Lignite | 3.1 | 2.7 | -0.4 |
        | Spain | Solar | 57.5 | 62.5 | +5 |
        | Spain | Wind | 29.1 | 26 | -3 |
        | Spain | Natural Gas | 2.4 | 2 | -0.4 |
        | Spain | Hydro | 6.8 | 5.8 | -0.9 |
        | Spain | Battery | 0.9 | 0.8 | -0.1 |
        | Spain | Oil | 0.4 | 0.4 | -0.1 |
        | Spain | Solid Biomass | 0.5 | 0.4 | -0.1 |
        | Spain | Waste | 0.2 | 0.2 | +0 |
        | Spain | Hard Coal | 0.7 | 0.6 | -0.1 |
        | Spain | Nuclear | 0.4 | 0.3 | -0.1 |
        | Spain | Lignite | 0.3 | 0.2 | +0 |
        | Spain | Hydrogen Storage | 0.3 | 0.3 | +0 |
        | Spain | Heat Storage | 0.5 | 0.4 | -0.1 |
        | Sweden | Solar | 14.7 | 14.5 | -0.2 |
        | Sweden | Wind | 38.5 | 39.3 | +0.8 |
        | Sweden | Natural Gas | 0.4 | 0.4 | +0 |
        | Sweden | Hydro | 28.2 | 27.8 | -0.5 |
        | Sweden | Battery | 6.4 | 6.4 | +0 |
        | Sweden | Oil | 1.9 | 1.9 | +0 |
        | Sweden | Solid Biomass | 5.9 | 5.8 | -0.1 |
        | Sweden | Waste | 1.6 | 1.6 | +0 |
        | Sweden | Hard Coal | 0.3 | 0.3 | +0 |
        | Sweden | Nuclear | 1.9 | 1.9 | +0 |
        | Switzerland | Solar | 7.8 | 7.8 | +0 |
        | Switzerland | Wind | 0.5 | 0.5 | +0 |
        | Switzerland | Biogas | 0.2 | 0.2 | +0 |
        | Switzerland | Natural Gas | 0.3 | 0.3 | +0 |
        | Switzerland | Hydro | 86.6 | 86.7 | +0.1 |
        | Switzerland | Battery | 1 | 1 | +0 |
        | Switzerland | Solid Biomass | 0.7 | 0.6 | +0 |
        | Switzerland | Waste | 1.6 | 1.6 | +0 |
        | Switzerland | Nuclear | 1 | 1 | +0 |
        | Switzerland | Hydrogen Storage | 0.2 | 0.2 | +0 |
        | Switzerland | Mechanical Storage | 0.2 | 0.2 | +0 |
        | Ukraine | Solar | 57.9 | 57.5 | -0.3 |
        | Ukraine | Wind | 7.9 | 7.8 | +0 |
        | Ukraine | Natural Gas | 5.6 | 6.1 | +0.5 |
        | Ukraine | Hydro | 5.6 | 5.6 | +0 |
        | Ukraine | Battery | 1.1 | 1.1 | +0 |
        | Ukraine | Hard Coal | 11.2 | 11.2 | -0.1 |
        | Ukraine | Nuclear | 10.7 | 10.6 | -0.1 |
        | United Kingdom | Solar | 47.8 | 47.5 | -0.4 |
        | United Kingdom | Wind | 29 | 29.7 | +0.7 |
        | United Kingdom | Biogas | 0.2 | 0.2 | +0 |
        | United Kingdom | Natural Gas | 4.1 | 4.1 | -0.1 |
        | United Kingdom | Hydro | 4.1 | 4 | -0.1 |
        | United Kingdom | Battery | 8.2 | 8.1 | -0.1 |
        | United Kingdom | Oil | 0.3 | 0.3 | +0 |
        | United Kingdom | Solid Biomass | 1.2 | 1.2 | +0 |
        | United Kingdom | Waste | 2.3 | 2.3 | +0 |
        | United Kingdom | Hard Coal | 1 | 0.9 | +0 |
        | United Kingdom | Nuclear | 1.3 | 1.3 | +0 |
        | United Kingdom | Hydrogen Storage | 0.3 | 0.3 | +0 |
        | United Kingdom | Geothermal | 0 | 0 | +0 |
        | United Kingdom | Mechanical Storage | 0 | 0 | +0 |

Capacities are the sum over all entries in the file, including planned and retired plants, in GW. The delta in brackets is in % of the old version, left out if the old version has none. The change of entries is a net change, so equal numbers of added and removed entries cancel out. An entry is complete if Fueltype, Technology, Set, Capacity, DateIn, lat and lon are all filled. Oldest and newest are the entries with the earliest and latest DateIn (year) over all entries of the new version, including planned plants. Ties go to the larger capacity, then to the name. Capacity shares are in % of the capacity of the country, their changes in percentage points (pp). The capacity share table leaves out the fuel types a country has entries of in neither version and the countries with capacity in neither version. Entry shares are in % of the entries of the country, their changes in percentage points (pp). The entry share table leaves out the fuel types a country has entries of in neither version. Blank cells are undefined, e.g. for a group missing in one version. Click a column header to sort. Shading marks the size of the change in classes of one decade, as in the overview figure.
{: .table-notes }

## By fuel type

The change per fuel type, with one tab per table. A figure shows old, new and change, the table below it is collapsed and sorted by a click on a column header.

The fuel types with the largest capacity change are Wind (+115.2 GW, +30 873 entries), Solar (+30.7 GW, +74 716 entries), Natural Gas (+5.8 GW, +5 450 entries).

=== "Capacity"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_fueltype/capacity.html" title="By fuel type: Capacity" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 464px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Fueltype | Capacity old [GW] | Capacity new [GW] | Capacity delta [GW] |
        | :-- | --: | --: | --: |
        | Battery | 31.325 | 32.538 | +1.213 (+3.9%) |
        | Biogas | 4.145 | 8.87 | +4.725 (+114%) |
        | Geothermal | 0.98 | 0.98 | +0 (+0%) |
        | Hard Coal | 186.85 | 187.654 | +0.803 (+0.4%) |
        | Heat Storage | 1.728 | 1.728 | +0 (+0%) |
        | Hydro | 218.722 | 222.707 | +3.985 (+1.8%) |
        | Hydrogen Storage | 0.044 | 0.044 | +0 (+0%) |
        | Lignite | 72.816 | 77.652 | +4.836 (+6.6%) |
        | Mechanical Storage | 0.01 | 0.01 | +0 (+0%) |
        | Natural Gas | 262.093 | 267.89 | +5.797 (+2.2%) |
        | Nuclear | 191.07 | 191.07 | +0 (+0%) |
        | Oil | 25.582 | 27.118 | +1.536 (+6%) |
        | Other | 1.002 | 1.143 | +0.141 (+14.1%) |
        | Solar | 191.041 | 221.77 | +30.729 (+16.1%) |
        | Solid Biomass | 18.44 | 18.591 | +0.151 (+0.8%) |
        | Waste | 8.696 | 8.911 | +0.214 (+2.5%) |
        | Wind | 235.887 | 351.134 | +115.247 (+48.9%) |

=== "Entries"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_fueltype/entries.html" title="By fuel type: Entries" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 464px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Fueltype | Entries old | Entries new | Entries delta |
        | :-- | --: | --: | --: |
        | Battery | 4 838 | 6 117 | +1 279 (+26.4%) |
        | Biogas | 7 587 | 20 982 | +13 395 (+176.6%) |
        | Geothermal | 43 | 43 | +0 (+0%) |
        | Hard Coal | 323 | 329 | +6 (+1.9%) |
        | Heat Storage | 41 | 41 | +0 (+0%) |
        | Hydro | 5 871 | 6 946 | +1 075 (+18.3%) |
        | Hydrogen Storage | 68 | 68 | +0 (+0%) |
        | Lignite | 145 | 150 | +5 (+3.4%) |
        | Mechanical Storage | 5 | 5 | +0 (+0%) |
        | Natural Gas | 9 117 | 14 567 | +5 450 (+59.8%) |
        | Nuclear | 238 | 238 | +0 (+0%) |
        | Oil | 2 679 | 4 704 | +2 025 (+75.6%) |
        | Other | 118 | 152 | +34 (+28.8%) |
        | Solar | 124 849 | 199 565 | +74 716 (+59.8%) |
        | Solid Biomass | 746 | 1 155 | +409 (+54.8%) |
        | Waste | 415 | 488 | +73 (+17.6%) |
        | Wind | 15 878 | 46 751 | +30 873 (+194.4%) |

=== "Completeness"

    <div style="position: relative; margin-bottom: 1em">
    <p style="position: absolute; inset: 0; margin: 0; display: grid; place-items: center; color: #757575">Loading figure…</p>
    <iframe src="by_fueltype/completeness.html" title="By fuel type: Completeness" loading="lazy"
      style="position: relative; display: block; width: 100%; height: 464px; border: 0"></iframe>
    </div>

    ??? note "Table"

        | Fueltype | Complete old [%] | Complete new [%] | Complete delta [pp] |
        | :-- | --: | --: | --: |
        | Battery | 85.4 | 88 | +2.6 |
        | Biogas | 98.3 | 98.7 | +0.5 |
        | Geothermal | 93 | 93 | +0 |
        | Hard Coal | 76.2 | 75.7 | -0.5 |
        | Heat Storage | 0 | 0 | +0 |
        | Hydro | 93.6 | 94.5 | +0.9 |
        | Hydrogen Storage | 1.5 | 1.5 | +0 |
        | Lignite | 82.8 | 80.7 | -2.1 |
        | Mechanical Storage | 0 | 0 | +0 |
        | Natural Gas | 97.2 | 97.5 | +0.3 |
        | Nuclear | 100 | 100 | +0 |
        | Oil | 97.4 | 98.2 | +0.8 |
        | Other | 66.9 | 61.8 | -5.1 |
        | Solar | 32.2 | 31.9 | -0.3 |
        | Solid Biomass | 65 | 74.1 | +9.1 |
        | Waste | 69.4 | 72.5 | +3.1 |
        | Wind | 97.5 | 99.1 | +1.6 |

=== "Oldest and newest"

    | Fueltype | Oldest | Oldest DateIn | Newest | Newest DateIn |
    | :-- | :-- | --: | :-- | --: |
    | Battery | Speicher | 1923 | Blauhoff Maxus Kwh | 2030 |
    | Biogas | Slough Heat | 1918 | Erweiterung Biomassepaket | 2029 |
    | Geothermal | Valle Secolo Geothermal | 1981 | Zagocha Geothermal | 2028 |
    | Hard Coal | Wroclaw | 1901 | Pulawy Grupa Azoty | 2025 |
    | Hydro | Turgi | 1828 | Wasser | 2029 |
    | Hydrogen Storage | Utsira Island | 1970 | Utsira Island | 1970 |
    | Lignite | Berrenrath | 1917 | Ptolemaida Timepoint | 2023 |
    | Natural Gas | Nyiregyhazi | 1897 | Gt | 2031 |
    | Nuclear | Marcoule G1 | 1956 | Hinkley Point C1 | 2029 |
    | Oil | Umg | 1900 | Netzersatzanlage Nea | 2026 |
    | Other | Dampfturbine Aeg | 1943 | Kleines | 2026 |
    | Solar | Ei Pv | 1900 | Pv Anlage Landesfeuerwehrschule Harrislee Gebaude | 2036 |
    | Solid Biomass | Blackburn Meadows | 1921 | Stw Hv Bhkw Bio | 2031 |
    | Waste | Turbine | 1913 | Ersatzbrennstoff Ring | 2029 |
    | Wind | Ayssenes Le Truel Wind Farm | 1958 | Re Rendsburg Eckernforde | 2056 |

Capacities are the sum over all entries in the file, including planned and retired plants, in GW. The delta in brackets is in % of the old version, left out if the old version has none. The change of entries is a net change, so equal numbers of added and removed entries cancel out. An entry is complete if Fueltype, Technology, Set, Capacity, DateIn, lat and lon are all filled. Oldest and newest are the entries with the earliest and latest DateIn (year) over all entries of the new version, including planned plants. Ties go to the larger capacity, then to the name. Blank cells are undefined, e.g. for a group missing in one version. Click a column header to sort. Shading marks the size of the change in classes of one decade, as in the overview figure.
{: .table-notes }

## Checks

The number of entries that fail a data-quality check, and the category values found in only one version or not in the config. A non-zero count is a hint to inspect, not an error.

Entries of the new version fail these checks: Capacity missing or not positive (38), DateOut before DateIn (3), Repeated entries (4 336), Category value not in config (8 182).

| Check | old | new | delta |
| :-- | --: | --: | --: |
| Capacity missing or not positive | 38 | 38 | +0 |
| No coordinates | 0 | 0 | +0 |
| No country | 0 | 0 | +0 |
| DateOut before DateIn | 3 | 3 | +0 |
| Repeated entries | 0 | 4 336 | +4 336 |
| Category value not in config | 6 893 | 8 182 | +1 289 |

Capacity missing or not positive: no capacity or 0 MW or less. No coordinates: lat or lon missing. DateOut before DateIn: retired before commissioned. Repeated entries: the same Name, Country, Fueltype, Capacity, lat and lon as an earlier entry. Category value not in config: a value that is not a key of its mapping in the config (Fueltype in target_fueltypes, Technology in target_technologies, Set in target_sets).
{: .table-notes }

| Category | Value | Only in | Entries | Capacity [GW] |
| :-- | :-- | :-- | --: | --: |
| Fueltype / Technology | Solar / Combustion Engine | new | 1 | 0 |

Category values found in only one version, e.g. a renamed fuel type, with their entries and capacity in GW in that version.
{: .table-notes }

| Category | Value | Entries old | Entries new | Entries delta | Capacity old [GW] | Capacity new [GW] | Capacity delta [GW] |
| :-- | :-- | --: | --: | --: | --: | --: | --: |
| Set | Store | 6 863 | 8 152 | +1 289 | 198.669 | 200.381 | +1.712 |
| Technology | CAES | 1 | 1 | +0 | 0.001 | 0.001 | +0 |
| Technology | Csp | 31 | 31 | +0 | 2.074 | 2.074 | +0 |
| Technology | Fe | 1 | 1 | +0 | 0.01 | 0.01 | +0 |
| Technology | Flywheel | 3 | 3 | +0 | 0.004 | 0.004 | +0 |
| Technology | Laes | 1 | 1 | +0 | 0.005 | 0.005 | +0 |
| Technology | Li | 4 353 | 5 579 | +1 226 | 21.846 | 23.046 | +1.199 |
| Technology | Molten Salt | 17 | 17 | +0 | 1.069 | 1.069 | +0 |
| Technology | NaNiCl | 2 | 2 | +0 | 0.001 | 0.001 | +0 |
| Technology | NaS | 12 | 17 | +5 | 0.049 | 0.05 | +0.001 |
| Technology | NiCd | 20 | 22 | +2 | 0.009 | 0.009 | +0 |
| Technology | Pb | 41 | 64 | +23 | 0.125 | 0.132 | +0.007 |
| Technology | V | 5 | 5 | +0 | 0.004 | 0.004 | +0 |

Category values not in config, with their entries and capacity in GW in both versions.
{: .table-notes }

A non-zero count or a listed value is a hint to inspect, not an error.
