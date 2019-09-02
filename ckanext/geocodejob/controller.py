# encoding: utf-8

from ckan.plugins.toolkit import (
    ObjectNotFound,
    NotAuthorized,
    get_action,
    _,
    request,
    BaseController,
    abort,
    render,
    c,
    h,
)

from ckanext.geocodejob.plugin import maybe_schedule

class GeocodeJobController(BaseController):

    def geocoded_data(self, id, resource_id):
        u'''Show status and allow forced update'''

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


        if request.method == 'POST':
            num = get_action('datastore_run_triggers')(
                None, {'resource_id': resource_id})
            h.flash_success(_('Processed %d records') % num)
            maybe_schedule(c.resource)

            h.redirect_to(
                controller='ckanext.geocodejob.controller:GeocodeJobController',
                action='geocoded_data',
                id=id,
                resource_id=resource_id)

        return render(
            'package/geocoded_data.html',
            extra_vars={
                'pkg_dict': c.pkg_dict,
                'resource': c.resource,
            })
