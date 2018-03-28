# encoding: utf-8

from ckan.plugins.toolkit import (
    Invalid,
    ObjectNotFound,
    NotAuthorized,
    get_action,
    get_validator,
    _,
    request,
    response,
    BaseController,
    abort,
    render,
    c,
    h,
)
from ckanext.geocodejob.model import ResourceGeocodeData
from ckanext.geocodejob.plugin import maybe_schedule

class GeocodejobController(BaseController):

    def geocoded_data(self, id, resource_id):
        u'''geocoded data fields view: show/edit geocoded data fields'''

        try:
            # resource_edit_base template uses these
            c.pkg_dict = get_action('package_show')(
                None, {'id': id})
            c.resource = get_action('resource_show')(
                None, {'id': resource_id})
            rec = get_action('datastore_search')(None, {
                'resource_id': resource_id,
                'limit': 0})
        except (ObjectNotFound, NotAuthorized):
            abort(404, _('Resource not found'))

        fields = get_geocoded_fields()
        resource_fields = rec.get('fields')
        address_column_options = [{"text":"","value":""}]
        for resource_field in resource_fields:
            if resource_field.get('id') == '_id':
                continue
            address_column_options.append({"text":resource_field.get('id'),"value":resource_field.get('id')})


        if request.method == 'POST':
            data = dict(request.POST)
            for field in fields:
                key = field.get('field_name')
                value = data.get(key)
                if ResourceGeocodeData.exists(resource_id=resource_id,key=key):
                    gecodefield_obj = ResourceGeocodeData.get(resource_id=resource_id,key=key)
                    gecodefield_obj.value = value
                    gecodefield_obj.commit()
                else:
                    ResourceGeocodeData.create(resource_id=resource_id,key=key, value=value)
            maybe_schedule(c.resource)

            h.redirect_to(
                controller='ckanext.geocodejob.controller:GeocodejobController',
                action='geocoded_data',
                id=id,
                resource_id=resource_id)

        geocode_data_values = ResourceGeocodeData.get_geocode_data_values(resource_id)

        return render(
            'package/geocoded_data.html',
            extra_vars={'fields': fields,
                        'address_column_options':address_column_options,
                        'geocode_data_values':geocode_data_values})

def get_geocoded_fields():
    fields = [
          {
          "field_name": "address_column",
          "label": "Address Column",
          "preset": "select",
          "choices": [
            {
              "value": "",
              "text": ""
            }
            ]
          },
          {
          "field_name": "geocode_data",
          "label": "Geocode State",
          "preset": "select",
          "choices": [
            {
              "value": "",
              "text": ""
            },
            {
              "value": "geocode",
              "text": "Geocode"
            },
            {
              "value": "started",
              "text": "Started"
            },
            {
              "value": "done",
              "text": "Done"
            }
          ]
        },
        {
          "field_name": "geocoder",
          "label": "Geocoder",
          "preset": "select",
          "choices": [
            {
              "value": "",
              "text": ""
            },
            {
              "value": "mapzen",
              "text": "Mapzen"
            },
            {
              "value": "nyc_geoclient",
              "text": "NYC Geoclient"
            },
            {
              "value": "openstreetmap",
              "text": "Openstreetmap"
            }
          ]
        },
        {
          "field_name": "geocoded_resource_id",
          "label": "Geocoded Resource ID",
          "form_placeholder": "Leave blank to generate new resource if set to geocode"
        }
      ]
    return fields
