import setuptools
import os
import sys

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()


sys.path.append("coconnect/")
from _version import __version__ as version

    
setuptools.setup(
    name="carrot-tools", 
    author="CaRROT",
    version=version,
    author_email="calmacx@gmail.com",
    description="Python package for performing mapping of ETL to CDM ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/HDRUK/CaRROT-CDM",
    entry_points = {
        'console_scripts':[
            'carrot=pycdm.cli.cli:carrot'
        ],
    },
    packages=setuptools.find_packages(),
    extras_require = {
        'airflow':['apache-airflow'],
        'performance':['snakeviz'],
    },
    install_requires=required,
    package_data={'carrot': ['data/cdm/*','data/example/*/*','data/test/*/*','data/test/*/*/*']},
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
