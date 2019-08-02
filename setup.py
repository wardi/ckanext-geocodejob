#!/usr/bin/env/python
from setuptools import setup

setup(
    name='ckanext-geocodejob',
    version='0.1',
    description='',
    license='AGPL3',
    author='',
    author_email='',
    url='',
    namespace_packages=['ckanext'],
    install_requires=['ckanapi'],
    packages=['ckanext.geocodejob'],
    zip_safe=False,
    entry_points = """
        [ckan.plugins]
        geocode_job = ckanext.geocodejob.plugin:GeocodeJobPlugin
    """
)
