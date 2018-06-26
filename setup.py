from __future__ import absolute_import



from setuptools import setup, find_packages
from codecs import open


#with open('README.md', encoding='utf-8') as f:
#    long_description = f.read()

setup(
    name='powerplantmatching',
    version='0.10.0',
    author='Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)',
    author_email='hofmann@fias.uni-frankfurt.de',
    description='Toolset for generating and managing Power Plant Data',
#    long_description=long_description,
    url='https://github.com/FRESNA/powerplantmatching',
    license='GPLv3',
    packages=find_packages(exclude=['duke_binaries', 'Hydro aggregation.py']),
    include_package_data=True,
    install_requires=['numpy','scipy','pandas>=0.19.0','networkx>=1.10','pycountry', 'xlrd', 'seaborn', 
                      'pyyaml', 'requests', 'matplotlib', 'basemap', 'geopy', 'xlrd'],
    classifiers=[
#        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Natural Language :: English',
        'Operating System :: OS Independent',
    ])
