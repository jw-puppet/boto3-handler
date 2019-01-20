import boto3
import time
from flask import current_app

from components.controllers.util.environ import get_env
from components.models.Contact import Contact


class DynamoDBComponents(object):
    def __init__(self):
        super(DynamoDBComponents, self).__init__()
        try:
            self.client = boto3.client('dynamodb',
                                       aws_access_key_id=get_env('AWS_ACCESS_KEY_ID'),
                                       aws_secret_access_key=get_env('AWS_SECRET_ACCESS_KEY'),
                                       region_name=get_env('Region'))
            self.table_name = None
            self.hash_key = None
            self.hash_key_type = None
            self.range_key = None
            self.range_key_type = None
        except:
            raise ConnectionError()

    @staticmethod
    def decode_attr(attr: dict):
        key, val = attr.popitem()[0]
        if key == 'N':
            return int(val)
        else:
            return val

    def list_tables(self, limit=100):
        return self.client.list_tables(Limit=limit)

    def set_table(self, table_name: str):
        # This Operation Not support When Count(tables) > 100, because single_action "list_tables" limit 100.
        if table_name in self.list_tables()['TableNames']:
            self.table_name = table_name
            table_info = self.client.describe_table(TableName=self.table_name)['Table']
            for attr in table_info['KeySchema']:
                if attr['KeyType'] == 'HASH':
                    self.hash_key = attr['AttributeName']
                else:
                    self.range_key = attr['AttributeName']
            for attr in table_info['AttributeDefinitions']:
                if attr['AttributeName'] == self.hash_key:
                    self.hash_key_type = attr['AttributeType']
                else:
                    self.range_key_type = attr['AttributeType']
            return True
        else:
            return False

    def query(self, Key, Filter=None):
        last_evaluated_key = -1  # default val = -1
        query_result = []

        # Key -> KeyExpression
        Key[f':{self.hash_key}'] = Key[self.hash_key]
        Key.pop(self.hash_key)
        if self.range_key in Key:
            Key[f':{self.range_key}'] = Key[self.range_key]
            Key.pop(self.range_key)

        # Set query parameters
        params = {'TableName': self.table_name, 'ExpressionAttributeValues': Key,
                  'KeyConditionExpression': f'{self.hash_key} = :{self.hash_key}' + (
                  f' AND {self.range_key} = :{self.range_key}' if f':{self.range_key}' in Key else '')}

        # querying contacts
        while last_evaluated_key:
            if not last_evaluated_key == -1:
                params['ExclusiveStartKey'] = last_evaluated_key
            resp = self.client.query(**params)
            last_evaluated_key = resp.get('LastEvaluatedKey', None)
            query_result = query_result+resp['Items']

        return query_result

    def put_item(self, Item: dict):
        assert self.table_name
        self.client.put_item(TableName=self.table_name, Item=Item)
        return

    def update_item(self, Key: dict, ExpressionAttributeNames: dict, ExpressionAttributeValues: dict,
                    UpdateExpression: str):
        self.client.update_item(TableName=self.table_name,
                                Key=Key,
                                ExpressionAttributeNames=ExpressionAttributeNames,
                                ExpressionAttributeValues=ExpressionAttributeValues,
                                UpdateExpression=UpdateExpression
                                )

    def delete_item(self, Key: dict):
        assert self.table_name
        self.client.delete_item(TableName=self.table_name, Key=Key)


class DynamoDBComponentsForAnybirth(DynamoDBComponents):
    def __init__(self):
        super(DynamoDBComponentsForAnybirth, self).__init__()
        # assert current_app.config['DYNAMO_PRIVATE_CONTACT_TABLE_NAME']
        # self.set_table(current_app.config['DYNAMO_PRIVATE_CONTACT_TABLE_NAME'])
        self.set_table('anybirth_user_contacts_private')

    def update_item(self, Key: dict, UpdateAttributes: dict):
        assert UpdateAttributes
        expattr_names = DynamoDBComponentsForAnybirth.param_expattr_names(UpdateAttributes)
        expattr_vals = DynamoDBComponentsForAnybirth.param_expattr_vals(UpdateAttributes)

        update_exp = []
        for i in UpdateAttributes:
            update_exp.append(f'#{i} = :{i}')
        update_exp = 'SET ' + ', '.join(update_exp)
        self.client.update_item(TableName=self.table_name,
                                Key=Key,
                                ExpressionAttributeNames=expattr_names,
                                ExpressionAttributeValues=expattr_vals,
                                UpdateExpression=update_exp
                                )

    def param_key(self, partition_val: str, sort_val: int = None) -> dict:
        assert self.table_name and self.hash_key and self.range_key
        key_condition_expression = {self.hash_key: {self.hash_key_type: partition_val}}
        if sort_val is not None:
            key_condition_expression[self.range_key] = {self.range_key_type: str(abs(sort_val))}
        return key_condition_expression

    @staticmethod
    def param_expattr_names(attrs: dict):
        exp = dict()
        for i in attrs:
            exp.__setitem__('#' + i, i)
        return exp

    @staticmethod
    def param_expattr_vals(attrs: dict):
        exp = dict()
        for i in attrs:
            exp.__setitem__(':' + i, attrs[i])
        return exp

    def param_default_user_property(self, sub: str) -> dict:
        assert self.table_name and self.hash_key and self.range_key
        return {
            self.hash_key: {self.hash_key_type: f'{sub}_userProperty'},
            self.range_key: {self.range_key_type: str(0)},
            'auto_increment': {'N': str(1)},
            'child_count': {'N': str(0)},
            'last_synctime': {'N': str(round(time.time()))}
        }

    def param_new_contact(self, user_property: dict, contact: Contact) -> dict:
        assert self.table_name and self.hash_key and self.range_key
        return {
            self.hash_key: {self.hash_key_type: f"{contact.owner.get_sub()}_contact"},
            self.range_key: {self.range_key_type: user_property['auto_increment'][self.range_key_type]},
            'auto_increment': {'N': str(1)},
            'name': {'S': contact.get_name()},
            'phone': {'S': contact.get_phone()},
            'child_count': {'N': str(0)}
        }
