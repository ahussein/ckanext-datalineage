# -*- coding: utf-8 -*-
import ckan.plugins as plugins
import ckan.plugins.toolkit as tk

import logging

logger = logging.getLogger(__name__)


class DatalineagePlugin(plugins.SingletonPlugin, tk.DefaultDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IRoutes)

    # IRoutes
    def after_map(self, map):
        """
        Map dataset_lineage 
        """
        map.connect('dataset_lineage', '/dataset/lineage/{id}',
                  controller='ckanext.datalineage.controllers.datalineage:DataLineageController',
                  action='show_datalineage', ckan_icon='sitemap')
        return map
    
    def before_map(self, map):
        return map


    # IConfigurer

    def update_config(self, config_):
        """
        Add this plugin's templates dir to CKAN's extra_templates_paths, so
        that CKAN will use this plugin's custom templates
        """
        tk.add_template_directory(config_, 'templates')
        tk.add_public_directory(config_, 'public')
        tk.add_resource('fanstatic', 'datalineage')
    
    # Helpers
    def _modify_package_schema(self, schema):
        # our custom fields
        schema.update({
            # parent represents the model/process that procuded the dataset, it should be a dataset id
            'parent': [tk.get_validator('ignore_missing'),
                       tk.get_converter('convert_to_extras')],
            # producers are comma seperated list of datasets ids that produced the current dataset
            'producers': [tk.get_validator('ignore_missing'),
                          tk.get_validator('convert_to_extras')],
            
            # consumers are comma seperated list of datasets ids that produced using the current dataset
            'consumers': [tk.get_validator('ignore_missing'),
                          tk.get_validator('convert_to_extras')],

            # code that identify the dataset/activity
            'code': [tk.get_validator('ignore_missing'),
                          tk.get_validator('convert_to_extras')],

        })

        return schema


    # IDatasetForm
    def create_package_schema(self):
        """
        Returns the schema for validating new dataset dicts
        """
        # lets grab the default schema in out plugin
        schema = super(DatalineagePlugin, self).create_package_schema()
        logger.debug('Updating default dataset schema with datalineage fields')
        return self._modify_package_schema(schema)
        
    

    def update_package_schema(self):
        """
        Returns the schema for validating an update dataset dicts
        """
        schema = super(DatalineagePlugin, self).update_package_schema()
        return self._modify_package_schema(schema)    

    def show_package_schema(self):
        """
        Return a schema to validate datasets before they’re shown to the user.
        is used when the package_show() action is called, we want the default_show_package_schema to be updated to include our custom field. 
        This time, instead of converting to an extras field, we want our field to be converted from an extras field. 
        So we want to use the convert_from_extras() converter.

        """
        schema = super(DatalineagePlugin, self).show_package_schema()
        schema.update({
            # parent represents the model/process that procuded the dataset, it should be a dataset id
            'parent': [
                tk.get_converter('convert_from_extras'),
                tk.get_validator('ignore_missing'),
            ],
            # producers are comma seperated list of datasets ids that produced the current dataset
            'producers': [
                tk.get_converter('convert_from_extras'),
                tk.get_validator('ignore_missing'),
            ],
            
            # consumers are comma seperated list of datasets ids that produced using the current dataset
            'consumers': [
                tk.get_converter('convert_from_extras'),
                tk.get_validator('ignore_missing'),    
            ],

            # code that identify the dataset/activity
            'code': [
                tk.get_converter('convert_from_extras'),
                tk.get_validator('ignore_missing'),    
            ],

        })

        return schema


    def is_fallback(self):
        """
        Return True to register this plugin as the default handler for
        package types not handled by any other IDatasetForm plugin.
        """
        return True

    
    def package_types(self):
        """
        This plugin doesn't handle any special package types, it just
        registers itself as the default.
        """
        return []