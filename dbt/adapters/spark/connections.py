import binascii
import os
import pathlib
import urllib.request
from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, ConnectionState
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS

from dbt.adapters.spark import __version__

try:
    from pyhive import hive
    from TCLIService.ttypes import TOperationState as ThriftState
    from thrift.transport import THttpClient
except ImportError:
    ThriftState = None
    THttpClient = None
    hive = None
try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    import jaydebeapi
    import jpype
    from jaydebeapi import _DEFAULT_CONVERTERS, _java_to_py
except ImportError:
    jpype = None
    jaydebeapi = None

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Union

import sqlparams
from hologram.helpers import StrEnum

try:
    import ssl

    import sasl
    import thrift
    import thrift_sasl
    from thrift.transport.TSSLSocket import TSSLSocket
except ImportError:
    pass  # done deliberately: setting modules to None explicitly violates MyPy contracts by degrading type semantics

import base64
import time

logger = AdapterLogger("Spark")

JDBC_KYUUBI_JPYPE_SETUP = False

NUMBERS = DECIMALS + (int, float)


def _jdbc_kyuubi_init_jpype() -> None:
    global JDBC_KYUUBI_JPYPE_SETUP

    if JDBC_KYUUBI_JPYPE_SETUP:
        return

    def _convert_java_binary(rs, col):
        # https://github.com/originell/jpype/issues/71
        # http://stackoverflow.com/questions/5088671
        # https://github.com/baztian/jaydebeapi/blob/master/jaydebeapi/__init__.py
        # https://msdn.microsoft.com/en-us/library/ms378813(v=sql.110).aspx
        # http://stackoverflow.com/questions/2920364/checking-for-a-null-int-value-from-a-java-resultset  # noqa

        v = None
        time1 = time.time()
        try:
            # ---------------------------------------------------------------------
            # Method 1: 3578880 bytes in 21.7430660725 seconds =   165 kB/s
            # ---------------------------------------------------------------------
            # java_val = rs.getObject(col)
            # if java_val is None:
            #     return
            # t = str(type(java_val))
            # if t == "<class 'jpype._jarray.byte[]'>": ...
            # v = ''.join(map(lambda x: chr(x % 256), java_val))

            # ---------------------------------------------------------------------
            # Method 2: 3578880 bytes in 8.07930088043 seconds =   442 kB/s
            # ---------------------------------------------------------------------
            # java_val = rs.getObject(col)
            # if java_val is None:
            #     return
            # l = len(java_val)
            # v = bytearray(l)
            # for i in xrange(l):
            #     v[i] = java_val[i] % 256

            # ---------------------------------------------------------------------
            # Method 3: 3578880 bytes in 20.1435189247 seconds =   177 kB/s
            # ---------------------------------------------------------------------
            # java_val = rs.getObject(col)
            # if java_val is None:
            #     return
            # v = bytearray(map(lambda x: x % 256, java_val))

            # ---------------------------------------------------------------------
            # Method 4: 3578880 bytes in 0.48352599144 seconds = 7,402 kB/s
            # ---------------------------------------------------------------------
            j_hexstr = rs.getString(col)
            if rs.wasNull():
                return
            v = binascii.unhexlify(j_hexstr)

        finally:
            time2 = time.time()
            return v

    def _convert_java_bigstring(rs, col):
        v = str(rs.getCharacterStream(col))
        if rs.wasNull():
            return None
        return v

    def _convert_java_bigint(rs, col):
        # http://stackoverflow.com/questions/26899595
        # https://github.com/baztian/jaydebeapi/issues/6
        # https://github.com/baztian/jaydebeapi/blob/master/jaydebeapi/__init__.py
        # https://docs.oracle.com/javase/7/docs/api/java/math/BigInteger.html
        # http://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
        java_val = rs.getObject(col)
        if java_val is None:
            return
        v = getattr(java_val, "toString")()  # Java call: java_val.toString()
        return int(v)

    _DEFAULT_CONVERTERS.update(
        {
            "BIGINT": _convert_java_bigint,
            "BINARY": _convert_java_binary,  # overrides an existing one
            "BLOB": _convert_java_binary,
            "LONGVARBINARY": _convert_java_binary,
            "VARBINARY": _convert_java_binary,
            "LONGVARCHAR": _convert_java_bigstring,
            "LONGNVARCHAR": _convert_java_bigstring,
        }
    )

    if pathlib.Path(os.path.join(os.path.dirname(__file__), "jars", "kyuubi-hive-jdbc-shaded-1.7.0.jar")).exists():
        logger.debug("setup_jpype: jar exists")
    else:
        logger.debug("setup_jpype: jar does not exist")

        url = "https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-hive-jdbc-shaded/1.7.0/kyuubi-hive-jdbc-shaded-1.7.0.jar"

        logger.debug("setup_jpype: downloading jar...")
        os.makedirs(os.path.join(os.path.dirname(__file__), "jars"), exist_ok=True)

        urllib.request.urlretrieve(
            url, os.path.join(os.path.dirname(__file__), "jars", "kyuubi-hive-jdbc-shaded-1.7.0.jar")
        )
        logger.debug("setup_jpype: jar downloaded")
    # jpype.startJVM(
    #     jpype.getDefaultJVMPath(),
    #     "-Djava.class.path=%s" % os.path.join(os.path.dirname(__file__), "jars", "kyuubi-hive-jdbc-shaded-1.7.0.jar"),
    #     ignoreUnrecognized=True,
    #     convertStrings=True,
    # )

    # jpype.attachThreadToJVM()
    # jpype.java.lang.Thread.currentThread().setContextClassLoader(jpype.java.lang.ClassLoader.getSystemClassLoader())
    JDBC_KYUUBI_JPYPE_SETUP = True


def _build_odbc_connnection_string(**kwargs) -> str:
    return ";".join([f"{k}={v}" for k, v in kwargs.items()])


class SparkConnectionMethod(StrEnum):
    THRIFT = "thrift"
    HTTP = "http"
    ODBC = "odbc"
    SESSION = "session"
    JDBC_KYUUBI = "jdbc_kyuubi"


@dataclass
class SparkCredentials(Credentials):
    host: str
    method: SparkConnectionMethod
    database: Optional[str]  # type: ignore
    driver: Optional[str] = None
    cluster: Optional[str] = None
    endpoint: Optional[str] = None
    token: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 443
    auth: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    organization: str = "0"
    connect_retries: int = 0
    connect_timeout: int = 10
    use_ssl: bool = False
    server_side_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    @property
    def cluster_id(self):
        return self.cluster

    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

        if self.method == SparkConnectionMethod.ODBC:
            try:
                import pyodbc  # noqa: F401
            except ImportError as e:
                raise dbt.exceptions.DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[ODBC]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

        if self.method == SparkConnectionMethod.ODBC and self.cluster and self.endpoint:
            raise dbt.exceptions.DbtRuntimeError(
                "`cluster` and `endpoint` cannot both be set when" f" using {self.method} method to connect to Spark"
            )

        if (self.method == SparkConnectionMethod.HTTP or self.method == SparkConnectionMethod.THRIFT) and not (
            ThriftState and THttpClient and hive
        ):
            raise dbt.exceptions.DbtRuntimeError(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[PyHive]`"
            )

        if self.method == SparkConnectionMethod.JDBC_KYUUBI and not (jaydebeapi and jpype):  # and jpype.isJVMStarted()
            raise dbt.exceptions.DbtRuntimeError(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[jdbc_kyuubi]`"
            )

        if self.method == SparkConnectionMethod.SESSION:
            try:
                import pyspark  # noqa: F401
            except ImportError as e:
                raise dbt.exceptions.DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[session]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

    @property
    def type(self):
        return "spark"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return ("host", "port", "cluster", "endpoint", "schema", "organization")


class PyhiveConnectionWrapper(object):
    """Wrap a Spark connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug("Exception while closing cursor: {}".format(exc))
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Reaching into the private enumeration here is bad form,
        # but there doesn't appear to be any way to determine that
        # a query has completed executing from the pyhive public API.
        # We need to use an async query + poll here, otherwise our
        # request may be dropped after ~5 minutes by the thrift server
        STATE_PENDING = [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]

        STATE_SUCCESS = [
            ThriftState.FINISHED_STATE,
        ]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        self._cursor.execute(sql, bindings, async_=True)
        poll_state = self._cursor.poll()
        state = poll_state.operationState

        while state in STATE_PENDING:
            logger.debug("Poll status: {}, sleeping".format(state))

            poll_state = self._cursor.poll()
            state = poll_state.operationState

        # If an errorMessage is present, then raise a database exception
        # with that exact message. If no errorMessage is present, the
        # query did not necessarily succeed: check the state against the
        # known successful states, raising an error if the query did not
        # complete in a known good state. This can happen when queries are
        # cancelled, for instance. The errorMessage will be None, but the
        # state of the query will be "cancelled". By raising an exception
        # here, we prevent dbt from showing a status of OK when the query
        # has in fact failed.
        if poll_state.errorMessage:
            logger.debug("Poll response: {}".format(poll_state))
            logger.debug("Poll status: {}".format(state))
            raise dbt.exceptions.DbtDatabaseError(poll_state.errorMessage)

        elif state not in STATE_SUCCESS:
            status_type = ThriftState._VALUES_TO_NAMES.get(state, "Unknown<{!r}>".format(state))
            raise dbt.exceptions.DbtDatabaseError("Query failed with status: {}".format(status_type))

        logger.debug("Poll status: {}, query complete".format(state))

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class PyodbcConnectionWrapper(PyhiveConnectionWrapper):
    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        # pyodbc does not handle a None type binding!
        if bindings is None:
            self._cursor.execute(sql)
        else:
            # pyodbc only supports `qmark` sql params!
            query = sqlparams.SQLParams("format", "qmark")
            sql, bindings = query.format(sql, bindings)
            self._cursor.execute(sql, *bindings)


class JDBCKyuubiConnectionWrapper(object):
    """Wrap a Spark connection in a way that no-ops transactions"""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug("Exception while closing cursor: {}".format(exc))
        self.handle.close()
        jpype.detachThreadFromJVM()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        data = self._cursor.fetchall()
        return data

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Escape '?' by replacing with '\?' to avoid issues with Parameter is not set
        # error in Kyuubi JDBC driver <org.apache.kyuubi.jdbc.hive.KyuubiSQLException: Parameter # is unset>
        # Ref: https://github.com/apache/kyuubi/blob/master/kyuubi-hive-jdbc/src/main/java/org/apache/kyuubi/jdbc/hive/Utils.java#LL141C1-L141C1
        sql.replace("?", "\?")

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        try:
            self._cursor.execute(sql)
        except Exception as exc:
            logger.debug("Exception while executing query: {}".format(exc))
            raise dbt.exceptions.DbtRuntimeError(str(exc)) from exc

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class SparkConnectionManager(SQLConnectionManager):
    TYPE = "spark"

    SPARK_CLUSTER_HTTP_PATH = "/sql/protocolv1/o/{organization}/{cluster}"
    SPARK_SQL_ENDPOINT_HTTP_PATH = "/sql/1.0/endpoints/{endpoint}"
    SPARK_CONNECTION_URL = "{host}:{port}" + SPARK_CLUSTER_HTTP_PATH

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.DbtRuntimeError(msg)
            else:
                raise dbt.exceptions.DbtRuntimeError(str(exc))

    def cancel(self, connection):
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds, required):
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required when using the {} method" " to connect to Spark".format(key, method)
                )

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == SparkConnectionMethod.HTTP:
                    cls.validate_creds(creds, ["token", "host", "port", "cluster", "organization"])

                    # Prepend https:// if it is missing
                    host = creds.host
                    if not host.startswith("https://"):
                        host = "https://" + creds.host

                    conn_url = cls.SPARK_CONNECTION_URL.format(
                        host=host,
                        port=creds.port,
                        organization=creds.organization,
                        cluster=creds.cluster,
                    )

                    logger.debug("connection url: {}".format(conn_url))

                    transport = THttpClient.THttpClient(conn_url)

                    raw_token = "token:{}".format(creds.token).encode()
                    token = base64.standard_b64encode(raw_token).decode()
                    transport.setCustomHeaders({"Authorization": "Basic {}".format(token)})

                    conn = hive.connect(thrift_transport=transport)
                    handle = PyhiveConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.THRIFT:
                    cls.validate_creds(creds, ["host", "port", "user", "schema"])

                    if creds.use_ssl:
                        transport = build_ssl_transport(
                            host=creds.host,
                            port=creds.port,
                            username=creds.user,
                            auth=creds.auth,
                            kerberos_service_name=creds.kerberos_service_name,
                            password=creds.password,
                        )
                        conn = hive.connect(thrift_transport=transport)
                    else:
                        conn = hive.connect(
                            host=creds.host,
                            port=creds.port,
                            username=creds.user,
                            auth=creds.auth,
                            kerberos_service_name=creds.kerberos_service_name,
                            password=creds.password,
                        )  # noqa
                    handle = PyhiveConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.ODBC:
                    if creds.cluster is not None:
                        required_fields = [
                            "driver",
                            "host",
                            "port",
                            "token",
                            "organization",
                            "cluster",
                        ]
                        http_path = cls.SPARK_CLUSTER_HTTP_PATH.format(
                            organization=creds.organization, cluster=creds.cluster
                        )
                    elif creds.endpoint is not None:
                        required_fields = [
                            "driver",
                            "host",
                            "port",
                            "token",
                            "endpoint",
                        ]
                        http_path = cls.SPARK_SQL_ENDPOINT_HTTP_PATH.format(endpoint=creds.endpoint)
                    else:
                        raise dbt.exceptions.DbtProfileError(
                            "Either `cluster` or `endpoint` must set when" " using the odbc method to connect to Spark"
                        )

                    cls.validate_creds(creds, required_fields)

                    dbt_spark_version = __version__.version
                    user_agent_entry = f"dbt-labs-dbt-spark/{dbt_spark_version} (Databricks)"  # noqa

                    # http://simba.wpengine.com/products/Spark/doc/ODBC_InstallGuide/unix/content/odbc/hi/configuring/serverside.htm
                    ssp = {f"SSP_{k}": f"{{{v}}}" for k, v in creds.server_side_parameters.items()}

                    # https://www.simba.com/products/Spark/doc/v2/ODBC_InstallGuide/unix/content/odbc/options/driver.htm
                    connection_str = _build_odbc_connnection_string(
                        DRIVER=creds.driver,
                        HOST=creds.host,
                        PORT=creds.port,
                        UID="token",
                        PWD=creds.token,
                        HTTPPath=http_path,
                        AuthMech=3,
                        SparkServerType=3,
                        ThriftTransport=2,
                        SSL=1,
                        UserAgentEntry=user_agent_entry,
                        LCaseSspKeyName=0 if ssp else 1,
                        **ssp,
                    )

                    conn = pyodbc.connect(connection_str, autocommit=True)
                    handle = PyodbcConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.JDBC_KYUUBI:
                    cls.validate_creds(creds, ["host", "port", "user"])

                    _jdbc_kyuubi_init_jpype()
                    try:
                        # 'org.apache.kyuubi.jdbc.KyuubiHiveDriver', 'jdbc:hive2://kyuubi.dev.nftbank.tools:10009', ['young', ''], '/Users/phyyou/.pyenv/versions/3.9.15/envs/data-baseplate-3.9.15/lib/python3.9/site-packages/dbt/adapters/spark/jars/kyuubi-hive-jdbc-shaded-1.7.0.jar']
                        logger.debug(
                            f"""JDBC_KYUUBI: connection info: {["org.apache.kyuubi.jdbc.KyuubiHiveDriver", f"jdbc:hive2://{creds.host}:{creds.port}", [creds.user, creds.password] , os.path.join(os.path.dirname(__file__), "jars", "kyuubi-hive-jdbc-shaded-1.7.0.jar")]}"""
                        )
                        conn = jaydebeapi.connect(
                            "org.apache.kyuubi.jdbc.KyuubiHiveDriver",
                            f"jdbc:hive2://{creds.host}:{creds.port}",
                            [creds.user, creds.password if creds.password is not None else ""],
                            os.path.join(os.path.dirname(__file__), "jars", "kyuubi-hive-jdbc-shaded-1.7.0.jar"),
                        )
                    except Exception as e:
                        logger.debug("JDBC_KYUUBI: failed to connect", e)
                        raise e
                    handle = JDBCKyuubiConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.SESSION:
                    from .session import Connection  # noqa: F401
                    from .session import SessionConnectionWrapper

                    handle = SessionConnectionWrapper(Connection())
                else:
                    raise dbt.exceptions.DbtProfileError(f"invalid credential method: {creds.method}")
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise dbt.exceptions.FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectError("failed to connect") from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[type, str]) -> str:  # type: ignore
        """
        :param Union[type, str] type_code: The sql to execute.
            * type_code is a python type (!) in pyodbc https://github.com/mkleehammer/pyodbc/wiki/Cursor#description, and a string for other spark runtimes.
            * ignoring the type annotation on the signature for this adapter instead of updating the base class because this feels like a really special case.
        :return: stringified the cursor type_code
        :rtype: str
        """
        if isinstance(type_code, str):
            return type_code
        return type_code.__name__.upper()


def build_ssl_transport(host, port, username, auth, kerberos_service_name, password=None):
    transport = None
    if port is None:
        port = 10000
    if auth is None:
        auth = "NONE"
    socket = TSSLSocket(host, port, cert_reqs=ssl.CERT_NONE)
    if auth == "NOSASL":
        # NOSASL corresponds to hive.server2.authentication=NOSASL
        # in hive-site.xml
        transport = thrift.transport.TTransport.TBufferedTransport(socket)
    elif auth in ("LDAP", "KERBEROS", "NONE", "CUSTOM"):
        # Defer import so package dependency is optional
        if auth == "KERBEROS":
            # KERBEROS mode in hive.server2.authentication is GSSAPI
            # in sasl library
            sasl_auth = "GSSAPI"
        else:
            sasl_auth = "PLAIN"
            if password is None:
                # Password doesn't matter in NONE mode, just needs
                # to be nonempty.
                password = "x"

        def sasl_factory():
            sasl_client = sasl.Client()
            sasl_client.setAttr("host", host)
            if sasl_auth == "GSSAPI":
                sasl_client.setAttr("service", kerberos_service_name)
            elif sasl_auth == "PLAIN":
                sasl_client.setAttr("username", username)
                sasl_client.setAttr("password", password)
            else:
                raise AssertionError
            sasl_client.init()
            return sasl_client

        transport = thrift_sasl.TSaslClientTransport(sasl_factory, sasl_auth, socket)
    return transport


def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "pending" in message or "temporarily_unavailable" in message:
        return str(exc)
    else:
        return ""
