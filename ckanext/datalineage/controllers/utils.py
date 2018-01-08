"""
Utils module for datalineage controller
"""

from ckan.common import OrderedDict, _, json, request, c, response, config
import ckan.model as model
import ckan.lib.base as base
import ckan.logic as logic
from paste.deploy.converters import asbool

import logging

logger = logging.getLogger(__name__)
abort = base.abort
render = base.render

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action

def get_title_for_code(code):
    """
    Retrieves a package title from a given code

    @param code: Code of the package to retrieve
    """
    context = {'model': model, 'session': model.Session,
                   'user': c.user, 'for_view': True,
                   'auth_user_obj': c.userobj}

    q = 'extras_code:%s' % (code.replace(':', '\:'))
    search_extras = {}
    data_dict = {
        'q': q,
        'fq': q,
        'extras': search_extras,
        'include_private': asbool(config.get(
            'ckan.search.default_include_private', True)),
    }
    query = get_action('package_search')(context, data_dict)
    results = query['results'][0] if query['results'] else {}
    if not results:
        logger.error('No dataset found with code {}'.format(code))
    return results.get('name', code)
