# -*- coding: utf-8 -*-
from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages
import sys

_requires = []
_install_requires = []

# rdflib 
_requires.append('rdflib')
_install_requires.append('rdflib >= 2.4.2')

# simplejson
if sys.version_info[0:2] < (2, 6):
    _requires.append('simplejson')
    _install_requires.append('simplejson == 2.0.9')

setup(
      name = 'SPARQLWrapper',
      version = '1.4.2',
      description = 'SPARQL Endpoint interface to Python',
      long_description = 'This is a wrapper around a SPARQL service. It helps in creating the query URI and, possibly, convert the result into a more manageable format.',
      license = 'W3C SOFTWARE NOTICE AND LICENSE', #Should be removed by PEP  314
      author = "Ivan Herman, Sergio Fernandez, Carlos Tejo Alonso",
      author_email = "ivan at ivan-herman net, sergio.fernandez at fundacionctic org, carlos.tejo at fundacionctic org",
      url = 'http://sparql-wrapper.sourceforge.net/',
      download_url = 'http://sourceforge.net/projects/sparql-wrapper/files',
      platforms = ['any'], #Should be removed by PEP  314
      packages = ['SPARQLWrapper'],
      requires = _requires, # Used by distutils to create metadata PKG-INFO
      install_requires = _install_requires, #Used by setuptools to install the dependencies
      classifiers =  [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: W3C License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.5',
      ],
      keywords = 'python SPARQL',
      #requires_python = '>=2.5', # Future in PEP 345
      #scripts = ['ez_setup.py']
)

