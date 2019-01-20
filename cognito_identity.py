# -*- coding: utf-8 -*-
# http://docs.aws.amazon.com/cognito/latest/developerguide/authentication-flow.html

from __future__ import print_function

import boto3

from components.controllers.util.environ import get_env


class CognitoIdentityComponents:
    def __init__(self):
        try:
            self.UserPoolId = get_env('UserPoolId')
            self.AccountId = get_env('AccountId')
            self.IdentityPoolId = get_env("IdentityPoolId")
            self.client = boto3.client('cognito-identity',
                                       aws_access_key_id=get_env('AWS_ACCESS_KEY_ID'),
                                       aws_secret_access_key=get_env('AWS_SECRET_ACCESS_KEY'),
                                       region_name=get_env('Region'))
        except:
            raise ConnectionError()

    def get_login_attributes(self, provider_name, auth_resp):
        login_providers = {
            'Facebook': 'graph.facebook.com',
            'Cognito': 'cognito-idp.' + get_env('Region') + '.amazonaws.com/' + self.UserPoolId
        }
        return {login_providers[provider_name]: auth_resp}

    def get_id(self, data):  # id_token
        resp = self.client.get_id(
            AccountId=self.AccountId,
            IdentityPoolId=self.IdentityPoolId,
            Logins=data
        )
        return resp

    def get_open_id_token(self, data):
        resp = self.client.get_open_id_token(
            IdentityId=data.identity_id,
            Logins={
                data.auth_provider_name: data.id_token
            }
        )
        return resp['Token']

    def get_credentials_for_identity(self, data):
        resp = self.client.get_credentials_for_identity(
            IdentityId=data.identity_id,
            Logins=data.login_attributes
        )
        return resp

    def delete_identity_id(self, data):
        if not type(data) is list:
            data = [data]
        resp = self.client.delete_identities(IdentityIdsToDelete=data)
        return resp['UnprocessedIdentityIds']

    def rollback_cognito_idt(self, identity_id):
        self.client.delete_identity_id(identity_id)


class CognitoSTS(object):
    def __init__(self, data):
        self.session_token = data['Credentials']['SessionToken']
        self.access_key_id = data['Credentials']['AccessKeyId']
        self.secret_key = data['Credentials']['SecretKey']
        self.expiration = data['Credentials']['Expiration']
        super(CognitoSTS, self).__init__()
        return


class CognitoIdentity(object):
    def __init__(self, id_component, idp_component, login_attr=None, identity_id=None):
        self.idc = id_component
        self.idpc = idp_component
        self.identity_id = identity_id
        self.login_attributes = login_attr

        self.region = None
        self.sts = None
        self.session = None

        self.set_region(get_env('Region'))

        super(CognitoIdentity, self).__init__()
        return

    def set_identity_id(self, data):
        self.identity_id = data
        return

    def set_login_attributes(self, data):
        self.login_attributes = data
        return

    def set_region(self, data):
        self.region = data if data else 'ap-northeast-1'
        return

    def authenticate(self, by="Cognito", data=None):
        def cognito_authenticate():
            try:
                self.set_login_attributes(self.idc.get_login_attributes(by, data))
                self.set_identity_id(self.idc.get_id(self.login_attributes)['IdentityId'])
                return True
            except:
                return False

        if by == "Cognito":
            return cognito_authenticate()

    def init_cognito_session(self):
        if self.identity_id and self.login_attributes and self.region:
            try:
                self.sts = CognitoSTS(self.idc.get_credentials_for_identity(self))
                self.session = boto3.Session(
                    aws_access_key_id=self.sts.access_key_id,
                    aws_secret_access_key=self.sts.secret_key,
                    aws_session_token=self.sts.session_token,
                    region_name=self.region
                )
                return True
            except:
                return False
