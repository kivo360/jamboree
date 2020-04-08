from copy import copy
from pprint import pprint
from addict import Dict
"""
    Find manipulate price information dynamically like you would from a UI
"""


# Find all datasets by market
# This means get datasets by market type (as in a search query). Then get a list of them.
# We'll pprint a list
dd = Dict()
dd.typing.hello.my.myworld.war="two"
dd.joy = "boy"
dd.squirl="girl"
dd.typing.derpy.face = "goodboi"
dd.typing.peace.atown = "down"
dd.typing.peace.msg = "out"

pprint([
    {"item": dd},
    {"item": dd},
    {"item": dd},
    {"item": dd},
    {"item": dd},
])


# Load those load the actual datasets from database

# Attach to multi-datasets

# Find all multi-datasets by raw term

# Find all datasets by user

# Load multi-dataset after loading it