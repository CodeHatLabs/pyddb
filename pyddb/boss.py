import boto3

from pypool import Pool

from .helpers import EZQuery


item_classes = {}
def register_item_class(item_class):
    item_classes[item_class.ITEM_CLASS_NAME] = item_class
    return item_class


class DynamoDBBoss(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key,
                                region_name, table_name_prefix=''):
        self.session = boto3.Session(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = region_name
            )
        self.dynamodb = self.session.resource('dynamodb')
        self.table_name_prefix = table_name_prefix
        self.tables = {}

    def get_item(self, item_class, pk, sort=None):
        tbl = self.get_table(item_class.TABLE_NAME)
        key = {item_class.PARTITION_KEY_NAME: pk}
        if item_class.SORT_KEY_NAME:
            key[item_class.SORT_KEY_NAME] = sort
        resp = tbl.get_item(Key=key)
        item = resp.get('Item')
        return self.item_factory(item) if item else None

    def get_items(self, item_base_class, key_attr, key, range_attr=None,
                range_value_begins_with=None, sort_key_lambda=None, reverse=False):
        tbl = self.get_table(item_base_class.TABLE_NAME)
        kwargs = {
            'index_name': item_base_class.get_index_name(key_attr, range_attr),
            'reverse': reverse
            }
        if range_attr and range_value_begins_with:
            kwargs['range_attr'] = range_attr
            kwargs['op'] = 'begins_with'
            kwargs['range'] = '%s:' % range_value_begins_with
        items = EZQuery(tbl, key_attr, key, **kwargs)
        return self.make_items(items, sort_key_lambda, reverse)

    def get_table(self, table_name):
        if not table_name in self.tables:
            prefixed_name = \
                    f'{self.table_name_prefix}-{table_name}' \
                    if self.table_name_prefix \
                    else table_name
            self.tables[table_name] = self.dynamodb.Table(prefixed_name)
        return self.tables[table_name]

    def item_factory(self, item_dict):
        class Morph(object):
            def __init__(self, boss, item_dict):
                self.__class__ = item_classes[item_dict['item_class_name']]
                self.__dict__.update(item_dict)
                self._boss = boss
                self._table = boss.get_table(self.TABLE_NAME)
        return Morph(self, item_dict)

    def make_items(self, item_dict_list, sort_key_lambda=None, reverse=False):
        result = [self.item_factory(item_dict) for item_dict in item_dict_list]
        if sort_key_lambda:
            result.sort(key=sort_key_lambda, reverse=reverse)
        return result


class DynamoDBBossPool(Pool):

    def __init__(self, aws_access_key_id, aws_secret_access_key,
                                region_name, table_name_prefix='', **kwargs):
        super().__init__(**kwargs)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name_prefix = table_name_prefix

    def create_resource(self):
        return DynamoDBBoss(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.region_name,
            self.table_name_prefix
            )
