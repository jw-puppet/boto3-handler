# -*- coding: utf-8 -*-
from __future__ import print_function

import boto3
from botocore.exceptions import ClientError

from components.controllers.util.environ import get_env


class SESComponent(object):
    def __init__(self):
        try:
            self.UserPoolId = get_env('UserPoolId')
            self.ClientId = get_env('ClientId')
            self.client = boto3.client('cognito-idp',
                                   aws_access_key_id=get_env('AWS_ACCESS_KEY_ID'),
                                   aws_secret_access_key=get_env('AWS_SECRET_ACCESS_KEY'),
                                   region_name=get_env('Region'))
        except:
            raise ConnectionError()
