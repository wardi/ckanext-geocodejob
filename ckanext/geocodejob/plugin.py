from datetime import datetime
from random import randint
import requests
import time

import ckanapi
import ckan.plugins as p
import ckan.plugins.toolkit as toolkit

from ckanext.geocodejob import logic, auth
from ckanext.geocodejob.model.datastore import (
    any_requested_rows, requested_remove_batch, Session)

try:
    # CKAN 2.7 and later
    from ckan.common import config
except ImportError:
    # CKAN 2.6 and earlier
    from pylons import config


TRIGGER_METADATA_FIELD = 'geocode_data'  # XXX change me to the new metadata field
TRIGGER_METADATA_VALUE = 'start'
TRIGGER_METADATA_STARTED = 'inprogress'
TRIGGER_METADATA_DONE = 'done'
TRIGGER_METADATA_RESOURCE = 'geocoded_resource_id'

GEOCODED_RESOURCE_NAME_POSTFIX = ' (Geocoded Data)'

GEOCLIENT_API_ID = config.get('ckanext.geocodejob.geoclient_api_id', '')
GEOCLIENT_API_KEY = config.get('ckanext.geocodejob.geoclient_api_key', '')
GEOCLIENT_SOURCE_COUNTRY = config.ge('ckanext.geocodejob.geoclient_source_country', 'USA')
# from http://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer?f=pjson
GEOCLIENT_BATCH_SIZE = 150


class GeocodeJobPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')

    # IResourceController

    def after_create(self, context, res_dict):
        maybe_schedule(res_dict)

    def after_update(self, context, res_dict):
        maybe_schedule(res_dict)

    # IActions

    def get_actions(self):
        return {
            'geocodejob_create_tables': logic.geocodejob_create_tables,
            'geocodejob_drop_tables': logic.geocodejob_drop_tables,
            'datastore_create': logic.datastore_create,
            'datastore_delete': logic.datastore_delete,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'geocodejob_create_tables': auth.geocodejob_create_tables,
            'geocodejob_drop_tables': auth.geocodejob_drop_tables,
        }


def maybe_schedule(res_dict):
    if not res_dict.get('datastore_active', False):
        return

    if not any_requested_rows():
        return

    lc = ckanapi.LocalCKAN()  # running as site user
    ds = lc.action.datastore_search(resource_id=res_dict['id'], rows=0)

    geocode_fields = [
        (field['id'], field['info']['geocode'])
        for field in data_dict.get('fields', [])
        if field.get('info', {}).get('geocode')
    ]
    if not any(geo == 'lat' or geo == 'lng' for f, geo in geocode_fields):
        return

    p.toolkit.enqueue_job(geocode_resource, [res_dict['id']])


def esri_token():
    """
    generate new (time limited) token for esri api calls
    """
    resp = requests.get(
        'https://www.arcgis.com/sharing/oauth2/token',
        {'client_id': GEOCLIENT_API_ID, 'client_secret': GEOCLIENT_API_KEY},
    )
    return resp.json()['access_token']


def geocode_resource(res_id):
    """
    Job that will be run by a worker process at a later time
    """
    token = None
    session = Session()

    while True:
        with session.begin():
            batch = requested_remove_batch(session)
            if not batch:
                break
            if not token:
                token = esri_token()
            addresses = {
                'records': [
                    {'attributes': {'OBJECTID': i, 'SingleLine': addr}}
                    for i, addr in enumerate(batch)
                ]
            }
            resp = requests.post(
                'https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses',
                params={
                    'f':'json',
                    'token': token,
                    'addresses': addresses,
                    'sourceCountry': GEOCLIENT_SOURCE_COUNTRY,
                }
            )
            # responses come in arbitrary order
            lng_lat = {
                r['ResultID']: (r['X'], r['Y'])
                for r in resp.json()['locations']
            }
            insert_cached_rows(
                session,
                [
                    (addr, ) + lng_lat.get(i, (None, None))
                    for i, addr in enumerate(batch)
                ]
            )

    lc = ckanapi.LocalCKAN()  # running as site user
    # update missing values from cache
    res_dict = lc.action.datastore_run_triggers(resource_id=res_id)


def GEOCLIENT_streetAddress(streetAddress):
    try:
        url = 'https://api.cityofnewyork.us/geoclient/v1/search.json'
        params = {'input': streetAddress, 'app_id': GEOCLIENT_API_ID, 'app_key': GEOCLIENT_API_KEY, 'exactMatchMaxLevel': '6'}
        r = requests.get(url, params=params)
        results = r.json()['results'][0]['response']
        return results
    except:
        return []

def OPENSTREETMAP_streetAddress(streetAddress):
    url = 'https://nominatim.openstreetmap.org/search'
    params = {"q":streetAddress,"format":"json","limit":1}
    r = requests.get(url, params=params)
    if len(r.json()) == 0 :
    	return []
    else:
    	response = r.json()[0]
        results = [response['lon'],response['lat']]
        return results

