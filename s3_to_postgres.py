import warnings
from typing import List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block


# [docs]
AVAILABLE_METHODS = ['APPEND', 'REPLACE', 'UPSERT']




class S3ToPostgresOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Postgres

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToRedshiftOperator`

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
        If the AWS connection contains 'aws_iam_role' in ``extras``
        the operator will use AWS STS credentials with a token
        https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param column_list: list of column names to load
    :type column_list: List[str]
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    :param method: Action to be performed on execution. Available ``APPEND``, ``UPSERT`` and ``REPLACE``.
    :type method: str
    :param upsert_keys: List of fields to use as key on upsert action
    :type upsert_keys: List[str]
    """


# [docs]    template_fields = ('s3_bucket', 's3_key', 'schema', 'table', 'column_list', 'copy_options')


# [docs]    template_ext = ()


# [docs]    ui_color = '#99e699'


    def __init__(
        self,
        *,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        postgres_conn_id: str = 'postgres_default',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        column_list: Optional[List[str]] = None,
        copy_options: Optional[List] = None,
        autocommit: bool = False,
        method: str = 'APPEND',
        upsert_keys: Optional[List[str]] = None,
        **kwargs,
    ) -> None:

        if 'truncate_table' in kwargs:
            warnings.warn(
                """`truncate_table` is deprecated. Please use `REPLACE` method.""",
                DeprecationWarning,
                stacklevel=2,
            )
            if kwargs['truncate_table']:
                method = 'REPLACE'
            kwargs.pop('truncate_table', None)

        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.column_list = column_list
        self.copy_options = copy_options or []
        self.autocommit = autocommit
        self.method = method
        self.upsert_keys = upsert_keys

        if self.method not in AVAILABLE_METHODS:
            raise AirflowException(f'Method not found! Available methods: {AVAILABLE_METHODS}')

    def _build_copy_query(self, copy_destination: str, credentials_block: str, copy_options: str) -> str:
        column_names = "(" + ", ".join(self.column_list) + ")" if self.column_list else ''
        return f"""
                    COPY {copy_destination} {column_names}
                    FROM 's3://{self.s3_bucket}/{self.s3_key}'
                    credentials
                    '{credentials_block}'
                    {copy_options};
        """


    def execute(self, context) -> None:
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = S3Hook.get_connection(conn_id=self.aws_conn_id)

        credentials_block = None
        if conn.extra_dejson.get('role_arn', False):
            credentials_block = f"aws_iam_role={conn.extra_dejson['role_arn']}"
        else:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
            credentials = s3_hook.get_credentials()
            credentials_block = build_credentials_block(credentials)

        copy_options = '\n\t\t\t'.join(self.copy_options)
        destination = f'{self.schema}.{self.table}'
        copy_destination = f'#{self.table}' if self.method == 'UPSERT' else destination

        copy_statement = self._build_copy_query(copy_destination, credentials_block, copy_options)

        if self.method == 'REPLACE':
            sql = ["BEGIN;", f"DELETE FROM {destination};", copy_statement, "COMMIT"]
        elif self.method == 'UPSERT':
            keys = self.upsert_keys or postgres_hook.get_table_primary_key(self.table, self.schema)
            if not keys:
                raise AirflowException(
                    f"No primary key on {self.schema}.{self.table}. Please provide keys on 'upsert_keys'"
                )
            where_statement = ' AND '.join([f'{self.table}.{k} = {copy_destination}.{k}' for k in keys])

            sql = [
                f"CREATE TABLE {copy_destination} (LIKE {destination});",
                copy_statement,
                "BEGIN;",
                f"DELETE FROM {destination} USING {copy_destination} WHERE {where_statement};",
                f"INSERT INTO {destination} SELECT * FROM {copy_destination};",
                "COMMIT",
            ]

        else:
            sql = copy_statement

        self.log.info('Executing COPY command...')
        postgres_hook.run(sql, autocommit=self.autocommit)
        self.log.info("COPY command complete...")
