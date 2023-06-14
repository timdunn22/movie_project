import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'movies.settings'
from common_methods import merged_dicts, not_null_value, flatten
import pandas as pd


class DataToDb:

    def __init__(self, model_class, file_path, identity=None, foreign_models=None):
        self.model_class = model_class
        self.file_path = file_path
        self.foreign_models = foreign_models
        self.foreign_models_dict = dict()
        if self.foreign_models is not None:
            self.foreign_models_dict = merged_dicts(self.foreign_models)
        self.identity = identity
        self.identity_objects = None
        self.foreign_dict = dict()
        self.identity_values = None
        self.identity_fields = None
        self.create_objects = None
        self.update_objects = None
        self.created_df = None
        self.updated_df = None
        self.df = None
        self.pk_field = self.model_class._meta.pk.attname
        self.model_fields = [column for column in self.model_class._meta.local_fields if column != self.pk_field]
        if self.foreign_models is None:
            self.foreign_models = list()
        if self.identity is None:
            self.identity = list()

    def field_type(self, field_type):
        return [column.attname for column in self.model_fields if field_type in str(column.__class__)]

    def integer_fields(self):
        return self.field_type('IntegerField')

    def boolean_fields(self):
        return self.field_type('BooleanField')

    def set_foreign_dict(self):
        for key in self.foreign_models_dict.keys():
            self.foreign_dict[key] = self.get_related_model_objects(key)

    def get_related_model_objects(self, key):
        foreign_model_object = self.model_class._meta.get_field(key).related_model
        return foreign_model_object.objects.values(foreign_model_object._meta.pk.attname, self.foreign_models_dict[key])

    def value_converted(self, key, value):
        if key in self.boolean_fields() and value not in [None, 0, 1, True, False]:
            return False
        elif key in self.integer_fields() and f'_{self.pk_field}' not in key:
            try:
                int(value)
            except:
                return 0
        return value

    def set_db_df(self):
        self.identity_fields = self.get_identity_fields()
        self.identity_objects = self.model_class.objects.values(*self.identity_fields)
        return pd.DataFrame(data=self.identity_objects)

    def get_identity_fields(self):
        main_identity_fields = [field for field in self.identity if field not in self.foreign_models_dict.keys()]
        foreign_fields = [self.model_class._meta.get_field(field).attname for field
                          in self.foreign_models_dict.keys() if field in self.identity]
        return flatten([[self.pk_field], foreign_fields, main_identity_fields])

    def set_df(self):
        df = self.assign_foreign_models(pd.read_csv(self.file_path))
        df.drop(list(self.foreign_models_dict.keys()), axis=1, inplace=True)
        db_df = self.set_db_df()
        merge_fields = [field for field in self.identity_fields if field != self.pk_field]
        if db_df.size > 0:
            df = pd.merge(df, db_df, on=merge_fields, how='left')
            self.created_df = df.loc[df[self.pk_field].isnull() & df[merge_fields].notnull().values.all(1)]
            self.updated_df = df.loc[~df[self.pk_field].isnull() & df[merge_fields].notnull().values.all(1)]
        else:
            self.created_df = df
            self.updated_df = None

    def assign_foreign_models(self, df):
        for key in self.foreign_dict.keys():
            df = self.merge_assign_df(key, df)
        return df

    def merge_assign_df(self, key, df):
        foreign_column = self.model_class._meta.get_field(key).attname
        foreign_df = pd.DataFrame(data=self.foreign_dict.get(key))
        df = pd.merge(df, foreign_df, left_on=key, right_on=self.foreign_models_dict.get(key), how='left')
        foreign_pk = self.get_foreign_merge_column(df, key)
        df[foreign_column] = df[foreign_pk]
        df.drop(foreign_df.columns, axis=1, inplace=True)
        return df

    def get_foreign_merge_column(self, df, key):
        foreign_primary_key = self.model_class._meta.get_field(key).related_model._meta.pk.attname
        id_columns = [column for column in df.columns if column.startswith(f'{foreign_primary_key}_')]
        if len(id_columns) > 0:
            return id_columns[0]
        return foreign_primary_key

    def create_dict_from_row(self, row):
        the_dict = self.get_initial_dict(row)
        if len(the_dict.keys()) > 0:
            return self.model_class(**the_dict)

    def get_initial_dict(self, row):
        initial_dict = {key: value for key, value in dict(row).items() if not_null_value(value)}
        return {key: self.value_converted(key, value) for key, value in initial_dict.items()}

    def set_movie_object(self, df):
        row_results = df.apply(lambda row: self.create_dict_from_row(row), axis=1)
        if list(row_results) not in [None, [None], []]:
            return list(row_results)

    def set_movie_objects(self):
        self.create_objects = self.set_movie_object(self.created_df)
        if self.updated_df is not None:
            self.update_objects = self.set_movie_object(self.updated_df)

    def create_records(self):
        if self.create_objects is not None:
            self.model_class.objects.bulk_create(self.create_objects, batch_size=100000)

    def update_records(self):
        if self.update_objects is not None and len(self.get_update_fields()) > 0:
            self.model_class.objects.bulk_update(self.update_objects, fields=self.get_update_fields(),
                                                 batch_size=100000)

    def get_update_fields(self):
        return [column for column in self.updated_df.columns if column not in self.identity_fields]

    def fast_import(self):
        self.set_foreign_dict()
        self.set_df()
        self.set_movie_objects()
        self.create_records()
        self.update_records()
