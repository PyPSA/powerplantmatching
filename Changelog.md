# History of Changes


## Version 0.4.1 (02.08.19)

### Data structure
- abolish git lfs in the favour of direct url parsing
- store data in ~/.local/share/powerplantmatching (adjusted for different OS) 
- move necessary files to package_data in powerplantmatching folder (such as duke binaries, xml files etc.) 
- include [JRC Hydro Database](https://github.com/energy-modelling-toolkit/hydro-power-database) 

### Code
- get rid of mutual module imports
- speed up grouping (cleaning.py, matching.py)   
- revise/rewrite code in data.py
- enable switch for matching powerplants of the same country only (is now default, speeds up the matching and aggregation process significantly)  
- boil down plot.py which caused long import times
- get rid of config.py in the favour of core.py and accessor.py
- drop deprecated functions in collection.py which now only includes collect() and matched_data()
