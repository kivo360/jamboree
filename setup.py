# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

# packages = \
# ['jamboree',
#  'jamboree.base',
#  'jamboree.base.old',
#  'jamboree.base.processors',
#  'jamboree.base.processors.abstracts',
#  'jamboree.handlers',
#  'jamboree.handlers.abstracted',
#  'jamboree.handlers.abstracted.models',
#  'jamboree.handlers.complex',
#  'jamboree.handlers.default',
#  'jamboree.handlers.processors',
#  'jamboree.storage',
#  'jamboree.storage.databases',
#  'jamboree.storage.files',
#  'jamboree.storage.files.redisify',
#  'jamboree.utils',
#  'jamboree.utils.context',
#  'jamboree.utils.core',
#  'jamboree.utils.procedures',
#  'jamboree.utils.procedures.models',
#  'jamboree.utils.processors',
#  'jamboree.utils.support',
#  'jamboree.utils.support.events',
#  'jamboree.utils.support.metrics',
#  'jamboree.utils.support.search',
#  'jamboree.utils.support.storage']

package_data = \
{'': ['*']}

install_requires = \
['addict>=2.2.1,<3.0.0',
 'crayons>=0.3.0,<0.4.0',
 'cytoolz>=0.10.1,<0.11.0',
 'dill>=0.3.1,<0.4.0',
 'funtime>=0.4.7,<0.5.0',
 'gym>=0.17.1,<0.18.0',
 'json-tricks>=3.14.0,<4.0.0',
 'loguru>=0.4.1,<0.5.0',
 'lz4>=3.0.2,<4.0.0',
 'maya>=0.6.1,<0.7.0',
 'numpy',
 'pandas>=1.0.3,<2.0.0',
 'pandas_datareader>=0.8.1,<0.9.0',
 'pebble>=4.5.1,<5.0.0',
 'pytest>=5.4.1,<6.0.0',
 'redis==3.3.11',
 'sklearn>=0.0,<0.1',
 'torch>=1.4.0,<2.0.0',
 'torchvision>=0.5.0,<0.6.0',
 'ujson>=2.0.2,<3.0.0',
 'version_query>=1.1.0,<2.0.0', 
 'orjson']

setup_kwargs = {
    'name': 'jamboree',
    'version': '0.5.2',
    'description': 'A multi-layer event sourcing and general data library',
    'long_description': None,
    'author': 'Kevin Hill',
    'author_email': 'kah.kevin.hill@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': find_packages(),
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.6,<4.0',
}


setup(**setup_kwargs)
