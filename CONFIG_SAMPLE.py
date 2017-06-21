# -*- coding: utf-8 -*-
"""Sample config file. Edit and save it as CONFIG.py."""


class CONFIG(object):
    """This class provides configuration data"""
    HEADS = {
        'qa': ['qa_manager@company.com'],
        'dev': ['dev_manager@company.com']
    }
    GCP_PROJECT_PREFIX = 'company-'
    EMAIL_FROM = 'Cloud Team <cloud@company.com>'
    EMAIL_DOMAIN = 'company.com'
    AZURE_CLIENT_ID = 'xxx'
    AZURE_SECRET = 'xxx'
    AZURE_TENANT = 'xxx'
    AZURE_SUBSCRIPTION_ID = 'xxx'
