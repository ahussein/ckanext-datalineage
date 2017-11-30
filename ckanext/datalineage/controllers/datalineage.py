#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import json


logger = logging.getLogger(__name__)
abort = base.abort
render = base.render

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action


EXAMPLE_DATA = {
    "model_data": {
        "model_1": {
            "dateTime": "2014-01-01T00:00:00",
            "output_datasets": [
                "glues:ilr:metadata:dataset:capri"
            ],
            "description": "The CAPRI model is a comparative static global partial equilibrium model for the agricultural sector. It endogenously determins market balances&#44; area use and yields and many other variables for agricu ...",
            "organisation": "Institute for Food and Resource Economics&#44; Bonn University",
            "paramName": "model_1",
            "title": "CAPRI",
            "type": "usage",
            "info": "",
            "input_datasets": [
                "6479e718-3a61-45cb-b36a-015254f56d7a",
                "596c710f-1830-4582-a0ec-13627f370b2f",
                "1f057e8b-16f0-43ad-a788-860250a31625",
                "glues:lmu:metadata:dataset:promet"
            ]
        },
        "paramName": "models",
        "model_0": {
            "dateTime": "2014-01-01T00:00:00",
            "output_datasets": [
                "glues:kei:metadata:dataset:dart"
            ],
            "description": "The DART model is a multi&#45;region&#44; multi&#45;sector&#44; recursive dynamic CGE model of the world economy and was developed at the Kiel Institute for the World Economy to analyse international climate policies ...",
            "organisation": "Kiel institute for the World Economy",
            "paramName": "model_0",
            "title": "DART",
            "type": "usage",
            "info": "",
            "input_datasets": [
                "glues:lmu:metadata:dataset:promet"
            ]
        },
        "model_2": {
            "dateTime": "2012-01-01T00:00:00",
            "output_datasets": [
                "glues:lmu:metadata:dataset:promet"
            ],
            "description": "Potential Yield Unit&#58; Potential Yield &#91;t/ha&#93;.",
            "organisation": "Department of Geography&#44; LMU Munich",
            "paramName": "model_2",
            "title": "PROMET",
            "type": "lineage",
            "info": "",
            "input_datasets": [
                "c9844990-9a92-4be1-a04f-6cb12a048e05",
                "cc92dd0e-47d3-4cce-b358-d134fd607539",
                "f9b95aec-bada-4326-aa60-144032cc0240",
                "476cb529-a1c0-47e1-84e7-1494acee7eaa",
                "f3b06df1-4e00-4a29-8442-7104aef2601f",
                "f0b8bcb5-cdea-46da-a51b-972d81e10def"
            ]
        }
    },
    "usage": {
        "models": {
            "usage_model_ids": [
                "usage_model_0",
                "usage_model_1"
            ],
            "paramName": "usage_models"
        },
        "mod_ds_relations": {
            "map1": {
                "dataset_ids": [
                    "glues:ilr:metadata:dataset:capri"
                ],
                "paramName": "usage_models_1"
            },
            "map0": {
                "dataset_ids": [
                    "glues:kei:metadata:dataset:dart"
                ],
                "paramName": "usage_models_0"
            },
            "usage_model_0": "model_0",
            "usage_model_1": "model_1",
            "paramName": "mod_ds_relations"
        },
        "paramName": "usage"
    },
    "dataset_data": {
        "596c710f-1830-4582-a0ec-13627f370b2f": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "Agriculture&#44;Statistical units",
            "save": "",
            "description": "The annual Agricultural Outlook is a report prepared jointly by the Organisation for Economic Co&#45;operation and Development &#40;OECD&#41; and the Food and Agriculture Organisation &#40;FAO&#41; of the United Nations. ...",
            "organisation": "OECD&#45;FAO",
            "paramName": "596c710f-1830-4582-a0ec-13627f370b2f",
            "title": "OECD&#45;FAO Agricultural Outlook 2009&#45;2018",
            "type": "usage_input",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=596c710f-1830-4582-a0ec-13627f370b2f",
            "relations_csw": "",
            "vector": "false",
            "time": "",
            "linked_2_modelInput": 0,
            "info": "null"
        },
        "cc92dd0e-47d3-4cce-b358-d134fd607539": {
            "extent": "-180,-60;180,-60;180,90;-180,90",
            "keywords": "Bodenkunde&#44;Boden&#44;Bodenressourcen&#44;Bodenart&#44;Bodentextur&#44;Bodenkarte&#44;Bodenoekologie&#44;Bodenkarte",
            "save": "http://www.iiasa.ac.at/Research/LUC/External-World-soil-database/HTML/",
            "description": "The HWSD &#40;Harmonized World Soil Database&#41; is a 30 arc&#45;second raster database with over 16000 different soil mapping units that combines existing regional and national updates of soil information world ...",
            "organisation": "International Institute for Applied Systems Analysis &#40;IIASA&#41;",
            "paramName": "cc92dd0e-47d3-4cce-b358-d134fd607539",
            "title": "HWSD &#45; Harmonized World Soil Database",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=cc92dd0e-47d3-4cce-b358-d134fd607539",
            "relations_csw": " ",
            "vector": "false",
            "time": "",
            "info": ""
        },
        "f0b8bcb5-cdea-46da-a51b-972d81e10def": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "global input&#45;output tables",
            "save": "",
            "description": "The Global Trade Analysis Project\u0092s GTAP 7 Data Base is a fully documented&#44; publicly available global data base for 113 regions and 57 GTAP commodities for a single year &#40;2004&#41;. It combines detailed b ...",
            "organisation": "Center for Global Trade Analysis&#44; Purdue University.",
            "paramName": "f0b8bcb5-cdea-46da-a51b-972d81e10def",
            "title": "GTAP 7 Data Base",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=f0b8bcb5-cdea-46da-a51b-972d81e10def",
            "relations_csw": " ",
            "vector": "false",
            "time": "2004-01-01-2004-12-31",
            "info": ""
        },
        "f9b95aec-bada-4326-aa60-144032cc0240": {
            "extent": "-180,-58;180,-58;180,60;-180,60",
            "keywords": "Digitales Gelaendemodell&#44;Relief",
            "save": "http://www.dgadv.com/srtm30/",
            "description": "SRTM30 is a near&#45;global digital elevation model &#40;DEM&#41; comprising a combination of data from the Shuttle Radar Topography Mission&#44; flown in February&#44; 2000 and the U.S. Geological Survey&lsquo;s GTOPO30 data ...",
            "organisation": "NASA",
            "paramName": "f9b95aec-bada-4326-aa60-144032cc0240",
            "title": "USGS SRTM30",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=f9b95aec-bada-4326-aa60-144032cc0240",
            "relations_csw": " ",
            "vector": "false",
            "time": "2011-08-01-",
            "info": ""
        },
        "476cb529-a1c0-47e1-84e7-1494acee7eaa": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "Land&#44;Landbedeckung&#44;Landwirtschaftliche Nutzung",
            "save": "http://ionia1.esrin.esa.int/",
            "description": "The GlobCover Land Cover product is the highest resolution &#40;300 meters&#41; Global Land Cover product ever produced and independently validated&#44; derived from an automatic and regionally&#45;tuned classificati ...",
            "organisation": "ESA",
            "paramName": "476cb529-a1c0-47e1-84e7-1494acee7eaa",
            "title": "Globcover 2009",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=476cb529-a1c0-47e1-84e7-1494acee7eaa",
            "relations_csw": " ",
            "vector": "false",
            "time": "",
            "info": ""
        },
        "f3b06df1-4e00-4a29-8442-7104aef2601f": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "Statistische Einheiten",
            "save": "",
            "description": "Regions for sector modelling at a global scale",
            "organisation": "TU Dresden",
            "paramName": "f3b06df1-4e00-4a29-8442-7104aef2601f",
            "title": "GLUES Regions",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=f3b06df1-4e00-4a29-8442-7104aef2601f",
            "relations_csw": " ",
            "vector": "false",
            "time": "2000-01-01-2060-12-31",
            "info": ""
        },
        "glues:ilr:metadata:dataset:capri": {
            "extent": "180,-90;-180,-90;-180,90;180,90",
            "keywords": "CAPRI&#44;Institute for Food and Resource Economics&#44;Bonn University&#44;agricultural product&#44;agricultural economics&#44;agricultural land",
            "save": "",
            "description": "The CAPRI model is a comparative static global partial equilibrium model for the agricultural sector. It endogenously determins market balances&#44; area use and yields and many other variables for agricu ...",
            "organisation": "Institute for Food and Resource Economics&#44; Bonn University",
            "paramName": "glues:ilr:metadata:dataset:capri",
            "title": "CAPRI",
            "type": "usage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=glues:ilr:metadata:dataset:capri",
            "relations_csw": "",
            "vector": "false",
            "time": "2010-01-01-2030-12-31",
            "linked_2_modelInput": 0,
            "info": ""
        },
        "c9844990-9a92-4be1-a04f-6cb12a048e05": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "ECHAM5 SRES&#45;A1B",
            "save": "",
            "description": "ECHAM5 &#40;A1B&#41; modeloutput with a spatial resolution of 0.5625°. &#40;Parameters&#58; Temperatur 2m&#44; Precipitation&#44; Snowfall&#44; Incoming Shortwave Radiation&#44; Incoming Longwave Radiation&#44; Dewpoint Temperature&#44; Su ...",
            "organisation": "Ocean Circulation &#38; Climate Dynamics Marine Meteorology Helmholtz&#45;Zentrum fuer Ozeanforschung Kiel &#40;GEOMAR&#41;",
            "paramName": "c9844990-9a92-4be1-a04f-6cb12a048e05",
            "title": "ECHAM5 A1B Scenario 1961&#45;2040; 2071&#45;2100",
            "type": "lineage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=c9844990-9a92-4be1-a04f-6cb12a048e05",
            "relations_csw": " ",
            "vector": "false",
            "time": "1971-01-01-1990-12-31",
            "info": ""
        },
        "glues:kei:metadata:dataset:dart": {
            "extent": "180,-90;-180,-90;-180,90;180,90",
            "keywords": "DART&#44;Kiel institute for the World Economy",
            "save": "",
            "description": "The DART model is a multi&#45;region&#44; multi&#45;sector&#44; recursive dynamic CGE model of the world economy and was developed at the Kiel Institute for the World Economy to analyse international climate policies ...",
            "organisation": "Kiel institute for the World Economy",
            "paramName": "glues:kei:metadata:dataset:dart",
            "title": "DART",
            "type": "usage",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=glues:kei:metadata:dataset:dart",
            "relations_csw": "",
            "vector": "false",
            "time": "2007-01-01-2030-12-31",
            "linked_2_modelInput": 0,
            "info": ""
        },
        "paramName": "datasets",
        "6479e718-3a61-45cb-b36a-015254f56d7a": {
            "extent": "-180,-90;180,-90;180,90;-180,90",
            "keywords": "Statistical units",
            "save": "",
            "description": "Food and Agriculture Organization of the United Nations Statistical database provides time&#45;series and cross sectional data from over 200 countries&#44; covering statistics on agriculture&#44; nutrition&#44; food ...",
            "organisation": "FOOD AND AGRICULTURE ORGANIZATION OF THE UNITED NATIONS",
            "paramName": "6479e718-3a61-45cb-b36a-015254f56d7a",
            "title": "FAOSTAT Database domains",
            "type": "usage_input",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=6479e718-3a61-45cb-b36a-015254f56d7a",
            "relations_csw": "",
            "vector": "false",
            "time": "1961-01-01-2010-12-31",
            "linked_2_modelInput": 0,
            "info": "null"
        },
        "1f057e8b-16f0-43ad-a788-860250a31625": {
            "extent": "180,-90;-180,-90;-180,90;180,90",
            "keywords": "EUROSTAT dataset",
            "save": "",
            "description": "Summary&#58; EUROSTAT Statistical database provides time&#45;series data from European Union. Data cover 27 Member States of the European Union&#44; while some of the indicators are provided for other countries&#44; ...",
            "organisation": "EUROSTAT",
            "paramName": "1f057e8b-16f0-43ad-a788-860250a31625",
            "title": "EUROSTAT",
            "type": "usage_input",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=1f057e8b-16f0-43ad-a788-860250a31625",
            "relations_csw": "",
            "vector": "false",
            "time": "1965-01-01-2012-01-01",
            "linked_2_modelInput": 0,
            "info": "null"
        }
    },
    "paramName": "metaViz_data",
    "lineage_detail": {
        "statement": {
            "description": "",
            "paramName": "statement"
        },
        "paramName": "lineage_detail",
        "process_steps": {
            "process_step_0": {
                "dateTime": "2012-01-01T00:00:00",
                "description": "Simulation of potential yield on representative sampling units in a Glues Region for 16 Glues&#45;Plants",
                "paramName": "process_step_0",
                "rationale": "not set",
                "processor": "Department of Geography&#44; LMU Munich",
                "processing_list": {
                    "paramName": "processingList",
                    "processing_0": {
                        "runTimeParams": "",
                        "identifier": "PROMET",
                        "docs": {
                            "size": 1,
                            "paramName": "docs_0",
                            "doc_0": {
                                "date": "2009-07-22",
                                "paramName": "doc_0",
                                "alternateTitle": "",
                                "id": "",
                                "title": "Mauser&#44; W.&#44; Bach&#44; H. &#40;2009&#41;&#58; PROMET &#45; Large scale distributed hydrological modelling to study the impact of climate change on the water flows of mountain watersheds&#44; Journal of Hydrology.",
                                "others": "http://www.sciencedirect.com/science/article/pii/S0022169409004478"
                            }
                        },
                        "sw_refs": {
                            "paramName": "sw_refs_0"
                        },
                        "paramName": "processing_0"
                    }
                }
            },
            "paramName": "process_steps"
        }
    },
    "mapping_ids_uuids": {
        "usage_dataset_1": "glues:ilr:metadata:dataset:capri",
        "usage_dataset_0": "glues:kei:metadata:dataset:dart",
        "detail_0": "glues:lmu:metadata:dataset:promet",
        "usage_model_0": "model_0",
        "usage_model_1": "model_1",
        "paramName": "mapping_ids_uuids",
        "6479e718-3a61-45cb-b36a-015254f56d7a": "6479e718-3a61-45cb-b36a-015254f56d7a",
        "lineage_dataset_5": "f0b8bcb5-cdea-46da-a51b-972d81e10def",
        "lineage_dataset_3": "476cb529-a1c0-47e1-84e7-1494acee7eaa",
        "lineage_dataset_4": "f3b06df1-4e00-4a29-8442-7104aef2601f",
        "596c710f-1830-4582-a0ec-13627f370b2f": "596c710f-1830-4582-a0ec-13627f370b2f",
        "lineage_model_0": "model_2",
        "lineage_dataset_1": "cc92dd0e-47d3-4cce-b358-d134fd607539",
        "lineage_dataset_2": "f9b95aec-bada-4326-aa60-144032cc0240",
        "1f057e8b-16f0-43ad-a788-860250a31625": "1f057e8b-16f0-43ad-a788-860250a31625",
        "lineage_dataset_0": "c9844990-9a92-4be1-a04f-6cb12a048e05"
    },
    "detail_data": {
        "paramName": "detail",
        "glues:lmu:metadata:dataset:promet": {
            "extent": "-180,-90;-180,90;180,90;180,-90",
            "keywords": "LMU&#44;potential yield&#44;Sorghum&#44;Sugarcane&#44;Potatoe&#44;Cassava&#44;Sunflower&#44;Soy&#44;Rapeseed&#44;Oilpalm&#44;Groundnut&#44;Wheat&#44;Rye&#44;Rice&#44;Millet&#44;Maize&#44;Barley&#44;Crops&#44;Department fuer Geographie&#44; LMU Muenchen&#44;PROMET",
            "save": "",
            "description": "Potential Yield Unit&#58; Potential Yield &#91;t/ha&#93;.",
            "organisation": "Department fuer Geographie&#44; LMU Muenchen",
            "paramName": "glues:lmu:metadata:dataset:promet",
            "title": "PROMET",
            "type": "",
            "view": "http://catalog-glues.ufz.de/terraCatalog/Query/ShowCSWInfo.do?fileIdentifier=glues:lmu:metadata:dataset:promet",
            "relations_csw": "",
            "vector": "false",
            "time": "",
            "linked_2_modelInput": 0,
            "info": ""
        }
    }
}


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
        extra_vars = {}
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
                extra_vars['datalineage_wasgeneratedby'] = query['results'][0]
            else:
                extra_vars['datalineage_generates'] = query['results'][0]
            
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

            extra_vars['datalineage_producers'] = producers_info

            dataset_type = c.pkg_dict['type'] or 'dataset'
            extra_vars['dataset_type'] = dataset_type
        except NotFound:
            abort(404, _('Dataset not found'))
        except NotAuthorized:
            abort(403, _('Unauthorized to read dataset %s') % id)

        return render('package/datalineage.html',
                        # extra_vars = {'data': json.dumps(extra_vars)},
                        extra_vars = {'data': json.dumps(EXAMPLE_DATA)}
                      )


