"""
Controllers module
Implements required controllers for the data lineage extension
"""

from ckan.controllers.package import PackageController
from ckan.common import OrderedDict, _, json, request, c, response
import ckan.model as model
import ckan.lib.base as base
import ckan.logic as logic

import logging


logger = logging.getLogger(__name__)
abort = base.abort
render = base.render

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action


class DataLineageController(PackageController):
    """
    Data lineage controller
    """

    def show_datalineage(self, id):
        """
        Retrieves data lineage information for a specific package
        """

        context = {'model': model, 'session': model.Session,
                   'user': c.user, 'for_view': True,
                   'auth_user_obj': c.userobj}
        data_dict = {'id': id}
        try:
            c.pkg_dict = get_action('package_show')(context, data_dict)
            c.pkg = context['package']
            # c.package_activity_stream = get_action(
            #     'package_activity_list_html')(
            #     context, {'id': c.pkg_dict['id']})
            c.datalineage = 'hello'
            dataset_type = c.pkg_dict['type'] or 'dataset'
        except NotFound:
            abort(404, _('Dataset not found'))
        except NotAuthorized:
            abort(403, _('Unauthorized to read dataset %s') % id)

        return render('package/datalineage.html',
                      {'dataset_type': dataset_type})


