from datetime import datetime
import requests
import time

import ckanapi
import ckan.plugins as p

try:
    # CKAN 2.7 and later
    from ckan.common import config
except ImportError:
    # CKAN 2.6 and earlier
    from pylons import config


TRIGGER_METADATA_FIELD = 'geocode_data'  # XXX change me to the new metadata field
TRIGGER_METADATA_VALUE = 'geocode'
TRIGGER_METADATA_STARTED = 'started'
TRIGGER_METADATA_DONE = 'done'
TRIGGER_METADATA_RESOURCE = 'geocoded_resource_id'

GEOCODED_RESOURCE_NAME_POSTFIX = ' (Geocoded Data)'

MAPZEN_API_KEY = config.get('ckanext.geocodejob.mapzen_api_key', '')

GEOCLIENT_API_ID = config.get('ckanext.geocodejob.geoclient_api_id', '')
GEOCLIENT_API_KEY = config.get('ckanext.geocodejob.geoclient_api_key', '')


class GeocodeJobPlugin(p.SingletonPlugin):
    p.implements(p.IResourceController, inherit=True)

    def after_create(self, context, res_dict):
        maybe_schedule(res_dict)

    def after_update(self, context, res_dict):
        maybe_schedule(res_dict)


def maybe_schedule(res_dict):
    if res_dict.get(TRIGGER_METADATA_FIELD) != TRIGGER_METADATA_VALUE:
        return

    p.toolkit.enqueue_job(geocode_dataset, [res_dict['id']])


def geocode_dataset(res_id):
    """
    Job that will be run by a worker process at a later time
    """
    lc = ckanapi.LocalCKAN()  # running as site user
    res_dict = lc.action.resource_show(id=res_id)
    pkg_dict = lc.action.package_show(id=res_dict.get('package_id'))

    # wait for the dataset to leave draft
    if res_dict['state'] != 'active':
        return

    # don't run again if we've already processed this one
    if res_dict.get(TRIGGER_METADATA_FIELD) != TRIGGER_METADATA_VALUE:
        return

    # if this takes a long time, don't start another job while this one is going
    lc.call_action('resource_patch', {
        'id':res_id,
        TRIGGER_METADATA_FIELD: TRIGGER_METADATA_STARTED})

    ## dummy work here ##
    # get data from the resource to be geocoded
    search_result = lc.call_action('datastore_search', {'id':res_id, 'limit':50000})
    fields = search_result.get('fields')
    records = search_result.get('records')

    # check if the resource was previously geocoded
    geocoded_res_dict = []
    geocoded_res_id = res_dict.get(TRIGGER_METADATA_RESOURCE)
    if geocoded_res_id:
        try:
            geocoded_res_dict = lc.action.resource_show(id=geocoded_res_id)
        except:
            pass

    # set the field names and types
    data_schema = []
    valid_resource = False
    for field in fields:
        if field.get('id') == '_id':
            data_schema.append({'id':'ckan_id', 'type':'integer'})
        elif field.get('id') == 'streetAddress':
            data_schema.append(field)
            valid_resource = True
        else:
            data_schema.append(field)

    # continue if the streetAddress field exists
    if valid_resource:
        if res_dict.get('geocoder') == 'mapzen' or res_dict.get('geocoder') == 'openstreetmap':
            data_schema.append({ 'id' : 'latitude', 'type' : 'text' })
            data_schema.append({ 'id' : 'longitude', 'type' : 'text' })

        elif res_dict.get('geocoder') == 'nyc_geoclient':
            data_schema.append({ 'id' : 'latitude', 'type' : 'text' })
            data_schema.append({ 'id' : 'longitude', 'type' : 'text' })
            data_schema.append({ 'id' : 'bbl', 'type' : 'text' })
            data_schema.append({ 'id' : 'bblBoroughCode', 'type' : 'text' })
            data_schema.append({ 'id' : 'bblTaxBlock', 'type' : 'text' })
            data_schema.append({ 'id' : 'bblTaxLot', 'type' : 'text' })
            data_schema.append({ 'id' : 'bin', 'type' : 'text' })
            data_schema.append({ 'id' : 'neighborhood', 'type' : 'text' })
            data_schema.append({ 'id' : 'cityCouncilDistrict', 'type' : 'text' })
            data_schema.append({ 'id' : 'communityDistrict', 'type' : 'text' })
            data_schema.append({ 'id' : 'assemblyDistrict', 'type' : 'text' })
            data_schema.append({ 'id' : 'electionDistrict', 'type' : 'text' })

        datastore_dict = {
            'fields': data_schema,
            'primary_key': ['ckan_id'],
            'format': 'CSV'
        }

        if geocoded_res_dict:
            datastore_dict['resource_id'] = geocoded_res_id
        else:
            datastore_dict['resource'] = {
                'package_id': pkg_dict.get('id'),
                'name': res_dict.get('name')+GEOCODED_RESOURCE_NAME_POSTFIX,
                'description': 'completed {0}'.format(datetime.utcnow())
            }

        create_result = lc.call_action('datastore_create', datastore_dict)
        resource_id = create_result.get('resource_id')

        new_records = []
        row_count = 0

        for row in records:
            row_count = row_count+1
            current_record = {}
            for field in fields:
                if field.get('id') == '_id':
                    current_record['ckan_id'] = row.get('_id')
                else:
                    current_record[field.get('id')] = row.get(field.get('id'))

            if row.get('streetAddress'):
                # geocode using the streetAddres
                if res_dict.get('geocoder') == 'mapzen':
                    response = MAPZEN_streetAddress(row.get('streetAddress'))
                    if len(response) == 2:
                        current_record['latitude'] = response[1]
                        current_record['longitude'] = response[0]
                        new_records.append(current_record)

                if res_dict.get('geocoder') == 'openstreetmap':
                    response = OPENSTREETMAP_streetAddress(row.get('streetAddress'))
                    if len(response) == 2:
                        current_record['latitude'] = response[1]
                        current_record['longitude'] = response[0]
                        new_records.append(current_record)

                if res_dict.get('geocoder') == 'nyc_geoclient':
                    response = GEOCLIENT_streetAddress(row.get('streetAddress'))

                    current_record['latitude'] = response.get('latitude','')
                    current_record['longitude'] = response.get('longitude','')
                    current_record['bbl'] = response.get('bbl','')
                    current_record['bblBoroughCode'] = response.get('bblBoroughCode','')
                    current_record['bblTaxBlock'] = response.get('bblTaxBlock','')
                    current_record['bblTaxLot'] = response.get('bblTaxLot','')
                    current_record['bin'] = response.get('buildingIdentificationNumber','')
                    current_record['neighborhood'] = response.get('ntaName','')
                    current_record['cityCouncilDistrict'] = response.get('cityCouncilDistrict','')
                    current_record['communityDistrict'] = response.get('communityDistrict','')
                    current_record['assemblyDistrict'] = response.get('assemblyDistrict','')
                    current_record['electionDistrict'] = response.get('electionDistrict','')
                    new_records.append(current_record)

                time.sleep(randint(500,1500) / 1000)

            # upsert in batches of 5000 rows
            if row_count%5000 == 0:
                datastore_dict = {
                    'resource_id': resource_id,
                    'records': new_records,
                    'method': 'upsert'
                }
                lc.call_action('datastore_upsert', datastore_dict)
                new_records = []

        # upsert any remaining rows
        if new_records:
            datastore_dict = {
                'resource_id': resource_id,
                'records': new_records,
                'method': 'upsert'
            }
            lc.call_action('datastore_upsert', datastore_dict)

    # update source resource setting the job to done
    lc.call_action('resource_patch', {
        'id': res_id,
        TRIGGER_METADATA_FIELD: TRIGGER_METADATA_DONE,
        TRIGGER_METADATA_RESOURCE: resource_id
    })

    # update geocoded resource with time finished
    lc.call_action('resource_patch', {
        'id': resource_id,
        'description': 'completed {0}'.format(datetime.utcnow())
    })


def MAPZEN_streetAddress(streetAddress):
    try:
        url = 'https://search.mapzen.com/v1/search'
        params = {'api_key': MAPZEN_API_KEY, 'text': streetAddress, 'boundary.country': 'USA'}
        r = requests.get(url, params=params)
        results = r.json()['features'][0]['geometry']['coordinates']
        return results
    except:
        return []


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
