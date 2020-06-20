# -*- coding: utf-8 -*-
import os
import codecs
import sys
from shutil import rmtree
from setuptools import setup, find_packages, Command


here = os.path.abspath(os.path.dirname(__file__))


with open("README.md", "r") as fh:
    long_description = fh.read()


class UploadCommand(Command):
    """Support setup.py publish."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except FileNotFoundError:
            pass
        self.status("Building Source distribution…")
        os.system("{0} setup.py sdist bdist_wheel".format(sys.executable))
        self.status("Uploading the package to PyPi via Twine…")
        os.system("twine upload dist/*")
        sys.exit()



setup(
    name="jamboree",
    version="0.9.5",
    author="Kevin Hill",
    author_email="kah.kevin.hill@gmail.com",
    description="A multi-layer event sourcing and general data library. SQL, Search, Event Sourcing, and File/Model storage combined into one.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["jamboree"],
    install_requires=[
        "numpy",
        "scipy",
        'maya',
        "pandas-datareader",
        "json-tricks",
        "ujson",
        'gym',
        "orjson",
        "pebble",
        "cytoolz",
        "loguru",
        'redisearch',
        "lz4",
        "anycache",
        "hiredis",
        "eliot",
        "eliot-tree",
        "matplotlib",
        "pandas",
        "sklearn",
        "crayons",
        "creme",
        'pydantic',
        "yfinance",
        "version_query",
        "pandas",
        "cerberus",
        "addict",
    ],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    cmdclass={"upload": UploadCommand},
)