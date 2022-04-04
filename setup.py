from __future__ import absolute_import

from codecs import open

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="powerplantmatching",
    version="0.5.0",
    author="Fabian Hofmann (FIAS), Jonas Hoersch (KIT), Fabian Gotzens (FZ Jülich)",
    author_email="hofmann@fias.uni-frankfurt.de",
    description="Toolset for generating and managing Power Plant Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/FRESNA/powerplantmatching",
    license="GPLv3",
    #    packages=find_packages(include='matching_analysis'),
    packages=["powerplantmatching"],
    include_package_data=True,
    install_requires=[
        "numpy",
        "scipy",
        "pandas>=0.24.0",
        "networkx>=1.10",
        "pycountry",
        "xlrd",
        "seaborn",
        "pyyaml >=5.1.0",
        "requests",
        "matplotlib",
        "geopy",
        "xlrd",
        "unidecode",
        "entsoe-py >=0.3.1",
        "deprecation",
        "openpyxl",
    ],
    extras_require={
        "docs": [
            "numpydoc",
            "sphinx",
            "sphinx_rtd_theme",
            "nbsphinx",
            "sphinx-automodapi",
        ],
        "dev": ["pre-commit", "pytest", "pytest-cov"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        #        'Development Status :: 3 - Alpha',
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Operating System :: OS Independent",
    ],
)
