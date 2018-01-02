
## ckanext-geocodejob

Schedule a background geocoding job that attaches or upates a resource when
user sets a special metadata field.


## Requirements

**This plugin is compatible with CKAN 2.7 or later**

This plugin uses CKAN [background jobs](http://docs.ckan.org/en/latest/maintaining/background-tasks.html) that was introduced in CKAN 2.7

**Requires [ckanext-scheming](https://github.com/ckan/ckanext-scheming)**

Use the included dataset\_schema.json file for the schema. The resource\_fields section provides the fields necessary for this extension.


## Installation

To install ckanext-geocodejob, ensure you have installed ckanext-scheming:

1. Activate your CKAN virtual environment:
```
. /usr/lib/ckan/default/bin/activate
```

2. Download the extension's github repository:
```
cd /usr/lib/ckan/default/src
git clone https://github.com/Ontodia/ckanext-geocodejob.git
```

3. Install the extension into your virtual environment:
```
cd ckanext-geocodejob
python setup.py develop
```

4. Add your geocoding API key (MapZen) to the config ini file
```
ckanext.geocodejob.mapzen_api_key = your-mapzen-api-key
```


## Background Jobs
**Development**

Workers can be started using the [Run a background job worker](http://docs.ckan.org/en/latest/maintaining/paster.html#paster-jobs-worker) command:

paster --plugin=ckan jobs worker --config=/etc/ckan/default/development.ini

**Production**

In a production setting, the worker should be run in a more robust way. One possibility is to use Supervisor.

For more information on setting up background jobs using Supervisor click [here](http://docs.ckan.org/en/latest/maintaining/background-tasks.html#using-supervisor).
