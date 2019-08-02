from ckanext.geocodejob.model.datastore import create_tables, drop_tables
from ckan.plugins.toolkit import check_access


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
