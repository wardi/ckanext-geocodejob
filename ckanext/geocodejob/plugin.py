import datetime

import ckanapi

import ckan.plugins as p

TRIGGER_METADATA_FIELD = 'notes'  # XXX change me to the new metadata field
TRIGGER_METADATA_VALUE = 'geocode'
TRIGGER_METADATA_STARTED = 'started'
TRIGGER_METADATA_DONE = 'done'

GEOCODED_RESOURCE_NAME = 'geocoded-data'


class GeocodeJobPlugin(p.SingletonPlugin):
    p.implements(p.IPackageController)

    def after_create(self, context, pkg_dict):
        maybe_schedule(pkg_dict)
    
    def after_update(self, contexxt, pkg_dict):
        maybe_schedule(pkg_dict)


def maybe_schedule(pkg_dict):
    if pkg_dict.get(TRIGGER_METADATA_FIELD) != TRIGGER_METADATA_VALUE:
        return

    p.schedule_job(geocode_dataset, [pkg_dict['id']])


def geocode_dataset(pkg_id):
    """
    Job that will be run by a worker process at a later time
    """
    lc = ckanapi.LocalCKAN()  # running as site user
    pkg_dict = lc.action.package_show(id=pkg_id)
    
    # don't run again if we've already processed this one
    if pkg_dict.get(TRIGGER_METADATA_FIELD) != TRIGGER_METADATA_VALUE:
        return

    # if this takes a long time, don't start another job while
    # this one is going
    lc.call_action('package_patch', {
        'id':pkg_id,
        TRIGGER_METADATA_FIELD: TRIGGER_METADATA_STARTED})

    # dummy work here
    resource_fields = {
        u'package_id': pkg_id,
        u'name': GEOCODED_RESOURCE_NAME,
        u'description': u'completed {0}'.format(datetime.utcnow()),
    }

    for res in pkg_dict['resources']:
        if res['name'] == GEOCODED_RESOURCE_NAME:
            resource_fields['id'] = res['id']
            lc.call_action('resource_update', resource_fields)
            break
    else:
        lc.call_action('resource_create', resource_fields)

    lc.call_action('package_patch', {
        'id':pkg_id,
        TRIGGER_METADATA_FIELD: TRIGGER_METADATA_DONE})
