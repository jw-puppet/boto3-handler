# -*- coding: utf-8 -*-
from __future__ import print_function

import boto3
from botocore.exceptions import ClientError

from components.controllers.util.environ import get_env


class CognitoIDPComponent:
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

    @staticmethod
    def auth_param(username, password):  # data = models.UserWithPassword
        return {"USERNAME": username, "PASSWORD": password}

    @staticmethod
    def refresh_param(refresh_token):  # data = "REFRESH_TOKEN"
        return {"REFRESH_TOKEN": refresh_token}

    def user_exist(self, username):  # UserName
        try:
            resp = self.admin_get_user(username)
            if resp:
                return resp
            else:
                return False

        except ClientError:
            return False

    def sign_up(self, username, password, attributes):  # UserData, UserPoolAttributes
        response = self.client.sign_up(
            ClientId=self.ClientId,
            Username=username,
            Password=password,
            UserAttributes=attributes
        )
        return response

    def get_user(self, user_access_token):
        return self.client.get_user(AccessToken=user_access_token)

    def confirm_sign_up(self, username, confirmation_code):  # UserData, ConfirmCode
        response = self.client.confirm_sign_up(
            ClientId=self.ClientId,
            Username=username,
            ConfirmationCode=confirmation_code
        )
        return response

    def admin_get_user(self, username):
        response = self.client.admin_get_user(
            UserPoolId=self.UserPoolId,
            Username=username
        )
        return response

    def admin_confirm_by_username(self, data):
        self.client.admin_confirm_sign_up(
            UserPoolId=self.UserPoolId,
            Username=data
        )
        return

    def admin_initiate_auth(self, auth_flow, auth_parameters):
        response = self.client.admin_initiate_auth(
            UserPoolId=self.UserPoolId,
            ClientId=self.ClientId,
            AuthFlow=auth_flow,
            AuthParameters=auth_parameters
        )
        return response['AuthenticationResult'] if response else False

    def admin_disable_user(self, username):
        assert username
        if self.user_exist(username):
            self.client.admin_disable_user(
                UserPoolId=self.UserPoolId,
                Username=username
            )

    def admin_delete_user(self, username):
        assert username
        if self.user_exist(username):
            self.client.admin_delete_user(
                UserPoolId=self.UserPoolId,
                Username=username
            )

    def forgot_password(self, username):
        self.client.forgot_password(
            ClientId=self.ClientId,
            Username=username
        )
        return True

    def confirm_forgot_password(self, username, confirmation_code, new_password):
        self.client.confirm_forgot_password(
            ClientId=self.ClientId,
            Username=username,
            ConfirmationCode=confirmation_code,
            Password=new_password
        )
        return True

    def rollback_cognito_idp(self, username: str):
        try:
            self.client.admin_delete_user(username)
        finally:
            return True

    def admin_user_update_attr(self, username: str, attr_key: str, attr_val: str):
        assert type(username) is str
        assert type(attr_key) is str
        self.client.admin_update_user_attributes(
            UserPoolId=self.UserPoolId,
            Username=username,
            UserAttributes=[
                {
                    'Name': attr_key,
                    'Value': attr_val
                },
            ]
        )

    def change_password(self, prev_password: str, prop_password: str, access_token: str):
        assert prev_password and prop_password and access_token
        self.client.change_password(PreviousPassword=prev_password, ProposedPassword=prop_password, AccessToken=access_token)
        return