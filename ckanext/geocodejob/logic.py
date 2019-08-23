from ckanext.geocodejob.model.datastore import create_tables, drop_tables

from ckan.plugins.toolkit import check_access, chained_action, get_action
from ckanext.datastore.backend.postgres import identifier

def geocodejob_create_tables(context, data_dict):
    '''
    Create the geocode request and cache tables in the datastore
    '''
    check_access('geocodejob_create_tables', context, data_dict)
    create_tables()


def geocodejob_drop_tables(context, data_dict):
    '''
    Remove the geocode request and cache tables from the datastore
    '''
    check_access('geocodejob_drop_tables', context, data_dict)
    drop_tables()


@chained_action
def datastore_create(up_func, context, data_dict):
    '''
    Intercept geocode settings in data dictionary fields
    to create/reset trigger geocode trigger function for this resource
    '''
    up_dict = dict(data_dict)
    geocode_fields = [
        (field['id'], field['info']['geocode'])
        for field in data_dict.get('fields', [])
        if field.get('info', {}).get('geocode')
    ]
    if any(geo == 'lat' or geo == 'lng' for fid, geo in geocode_fields):
        _create_geocode_function(
            context,
            data_dict['resource_id'],
            geocode_fields
        )
        up_dict['triggers'] = data_dict.get('triggers', []) + [
            {'function': 'geocode_trigger_' + data_dict['resource_id']}]
    else:
        _delete_geocode_function(context, data_dict['resource_id'])

    rval = up_func(context, up_dict)
    return rval


@chained_action
def datastore_delete(up_func, context, data_dict):
    '''
    When completely removing datastore table make sure to remove
    any geocode function previously created
    '''
    rval = up_func(context, data_dict)

    if 'filters' not in data_dict:
        _delete_geocode_function(context, data_dict['resource_id'])

    return rval


def _delete_geocode_function(context, resource_id):
    get_action('datastore_function_delete')(
        dict(context, ignore_auth=True),
        {
            'name': 'geocode_trigger_' + resource_id,
            'if_exists': True,
        }
    )


def _create_geocode_function(context, resource_id, geocode_fields):
    '''
    create custom trigger for this resource ID for populating and
    requesting geocoded values in geocode_cache & geocode_request
    '''
    lat_keys = []
    lng_keys = []
    address_fields = {}
    for fid, geo in geocode_fields:
        if geo == 'lat':
            lat_keys.append(fid)
        elif geo == 'lng':
            lng_keys.append(fid)
        else:
            address_fields[geo] = fid

    lat_lng_unset = ' AND '.join(
        "(NEW.{name}::text = '') IS NOT FALSE".format(name=identifier(name))
        for name in lat_keys + lng_keys
    )
    assign_lat_lng = ';'.join(
        "NEW.{name} = lat".format(name=identifier(name))
        for name in lat_keys) + ';' + ';'.join(
        "NEW.{name} = lng".format(name=identifier(name))
        for name in lng_keys
    )
    full_address = "|| ', ' ||".join(
        "NEW.{name}::text".format(name=identifier(address_fields[key]))
        for key in ['address', 'city', 'postal'] if key in address_fields
    )

    lng_keys = (fid for fid, geo in geocode_fields if geo == 'lng')
    get_action('datastore_function_create')(
        dict(context, ignore_auth=True),
        {
            'name': u'geocode_trigger_' + resource_id,
            'or_replace': True,
            'rettype': u'trigger',
            'definition': u'''
                DECLARE
                    full_address text := {full_address};
                    lat numeric;
                    lng numeric;
                BEGIN
                    IF {lat_lng_unset} THEN
                        IF NOT exists(select latitude, longitude
                                from geocode_cache
                                where address = full_address) THEN
                            IF NOT exists(select address
                                    from geocode_request
                                    where address = full_address) THEN
                                insert into geocode_request values (full_address);
                            END IF;
                        ELSE
                            select latitude, longitude
                            into lat, lng
                            from geocode_cache
                                where address = full_address limit 1;
                            {assign_lat_lng};
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                '''.format(
                    assign_lat_lng=assign_lat_lng,
                    full_address=full_address,
                    lat_lng_unset=lat_lng_unset,
                ),
        }
    )
