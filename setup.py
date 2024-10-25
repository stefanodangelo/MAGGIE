"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the MAGGIE project.
"""
from setuptools import setup, find_packages

import os
import sys
sys.path.append('./src')
sys.path.append('./src/MAGGIE')

import datetime
import MAGGIE


# Utility function to read requirements
def parse_requirements(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line and not line.startswith('#')]
    
setup(
    name="MAGGIE",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=MAGGIE.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    url="https://databricks.com",
    author=os.environ["DEPLOYMENT_EMAIL"],
    description="wheel file based on MAGGIE/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=MAGGIE.main:main"
        ]
    },
    install_requires=parse_requirements('requirements.txt'),
)
