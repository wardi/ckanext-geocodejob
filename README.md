
## ckanext-geocodejob

Schedule a background geocoding job that attaches or upates a resource when
user sets a special metadata field.


## Requirements

Before installing ckanext-geocodejob, install the following:

* ckanext-scheming (https://github.com/ckan/ckanext-scheming)

Use the included dataset\_schema.json file for the schema. The resource\_fields section provides the fields necessary for this extension.


## Installation

To install ckanext-geocodejob, ensure you have installed ckanext-scheming:

1. Activate your CKAN virtual environment::

     . /usr/lib/ckan/default/bin/activate

2. Download the extension's github repository::

     cd /usr/lib/ckan/default/src
     git clone https://github.com/Ontodia/ckanext-geocodejob.git

3. Install the extension into your virtual environment::

     cd ckanext-geocodejob
     python setup.py develop

4. Add your geocoding API key (MapZen) to plugin.py
