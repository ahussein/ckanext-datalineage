"""
Controllers module
Implements required controllers for the data lineage extension
"""

from ckan.controllers.package import PackageController
from ckan.common import OrderedDict, _, json, request, c, response, config
import ckan.model as model
import ckan.lib.base as base
import ckan.logic as logic

import logging
from paste.deploy.converters import asbool


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

            # this is a DS
            if c.pkg_dict.get('parent'):
                q = 'extras_code:%s' % (c.pkg_dict.get('parent').replace(':', '\:'))
            else:
                # this is a process/activity/model
                q = 'extras_parent:%s' % (c.pkg_dict.get('code').replace(':', '\:'))

            search_extras = {}
            data_dict = {
                'q': q,
                'fq': q,
                'extras': search_extras,
                'include_private': asbool(config.get(
                    'ckan.search.default_include_private', True)),
            }
            query = get_action('package_search')(context, data_dict)

            if c.pkg_dict.get('parent'):
                c.datalineage_wesgeneratedby = query['results'][0]
            else:
                c.datalineage_generates = query['results'][0]
            
            # get the producers DSs
            producers = c.pkg_dict.get('producers', []) or query['results'][0].get('producers', [])
            producers_info = []
            for ds_code in producers.split(','):
                q = 'extras_code:%s' % (ds_code.replace(':', '\:'))
                data_dict = {
                'q': q,
                'fq': q,
                'extras': {},
                'include_private': asbool(config.get(
                    'ckan.search.default_include_private', True)),
                }
                query = get_action('package_search')(context, data_dict)
                if query['count'] == 0:
                    logger.warning('No result found for producer [%s] of package [%s]' % (ds_code, c.pkg_dict['code']) )
                else:
                    producers_info.append(query['results'][0])

            c.datalineage_producers = producers_info

            dataset_type = c.pkg_dict['type'] or 'dataset'
        except NotFound:
            abort(404, _('Dataset not found'))
        except NotAuthorized:
            abort(403, _('Unauthorized to read dataset %s') % id)

        return render('package/datalineage.html',
                      {'dataset_type': dataset_type})


