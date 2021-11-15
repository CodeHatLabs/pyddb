import boto3

from pypool import Pool


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
        tbl = self.get_table(item_class)
        key = {item_class.PARTITION_KEY_NAME: pk}
        if item_class.SORT_KEY_NAME:
            key[item_class.SORT_KEY_NAME] = sort
        resp = tbl.get_item(Key=key)
        item = resp.get('Item')
        return self.item_factory(item_class, item) if item else None

    def get_table(self, name_or_class):
        table_name = name_or_class \
            if type(name_or_class) == type('') \
            else name_or_class.TABLE_NAME
        if not table_name in self.tables:
            self.tables[table_name] = self.dynamodb.Table('%s-%s' % (
                                        self.table_name_prefix, table_name))
        return self.tables[table_name]

    def item_factory(self, item_class, dynamodb_item_dict):
        class Morph(object):
            def __init__(self, boss, item_class, dynamodb_item_dict):
                self.__class__ = item_class
                self.__dict__.update(dynamodb_item_dict)
                self._boss = boss
                self._table = boss.get_table(self)
        return Morph(self, item_class, dynamodb_item_dict)


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


