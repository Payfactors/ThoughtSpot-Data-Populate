import os
import datetime as dt
import pyodbc
from loguru import logger
import random
import time
from dotenv import load_dotenv

load_dotenv()

import pandas as pd
from typing import Any, Optional, Sequence, Mapping


class SQLServerClient:
    """Lightweight SQL Server helper using pyodbc.

    - Does NOT keep a persistent connection. Each call opens and closes its own connection.
    - Safe to instantiate multiple times for different servers/databases simultaneously.

    You can provide default connection parameters at construction time or override them per call.
    """

    def __init__(self, server, port, database, username, password
                 , encrypt="yes", trust_server_certificate="yes", timeout_seconds=60, autocommit=True):
        self.server = server
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.timeout_seconds = timeout_seconds
        self.autocommit =  self._to_bool(autocommit)
        self.connection = None

    def _build_connection_string(
        self,
        server,
        port,
        database,
        username,
        password,
        encrypt="yes",
        trust_server_certificate="yes",
        timeout_seconds=60,
    ) -> str:
        """Create a pyodbc connection string including host and port using SQL authentication only."""
        driver_val = "ODBC Driver 17 for SQL Server"

        if not server:
            raise ValueError("Server is required to build a SQL Server connection string")
        if not username or not password:
            raise ValueError("Username and password are required for SQL authentication")
        segments = [
            f"DRIVER={{{driver_val}}}",
            f"SERVER={server},{port}",
        ]
        if database:
            segments.append(f"DATABASE={database}")
        # SQL Authentication (no Trusted_Connection fallback)
        segments.append(f"UID={username}")
        segments.append(f"PWD={password}")
        if encrypt:
            segments.append(f"Encrypt={encrypt}")
        if trust_server_certificate:
            segments.append(f"TrustServerCertificate={trust_server_certificate}")
        if timeout_seconds:
            segments.append(f"Connection Timeout={int(timeout_seconds)}")
        return ";".join(segments)

    def connect(self) -> pyodbc.Connection:
        """Open and return a new pyodbc connection. Caller is responsible for closing it.

            Prefer using higher-level helpers which handle open/close automatically
            unless you specifically need a manual connection for a custom operation.
            """
        try:
            # logger.debug(f"Connecting to SQL Server: server={self.server} on database={self.database}")
            conn_string = self._build_connection_string(
                server=self.server,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                encrypt=self.encrypt,
                trust_server_certificate=self.trust_server_certificate,
                timeout_seconds=self.timeout_seconds,
            )
            conn = pyodbc.connect(conn_string, autocommit=self.autocommit)
            self.connection = conn
            # logger.info(f"Connection to SQL Server database {self.server}/{self.database} opened.")
            return self.connection
        except Exception as ex:
            sqlstate = ex.args[0] if ex.args else str(ex)
            raise logger.error(
                f"Error opening connection to SQL Server database {self.server},{self.port}/{self.database} by user {self.username}: {sqlstate}")

    def execute_query(self, query, params: Optional[Sequence[Any]] = None):
        try:
            """ Open a transient connection for each query, then close  """
            try:
                conn = self.connect()
                cursor = self.connection.cursor()
                if params is None:
                    cursor.execute(query)
                else:
                    normalized = self._normalize_params(params)
                    cursor.execute(query, normalized)
                records = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                # logger.info(f"Records length: {len(records)}, Columns length: {len([desc[0] for desc in cursor.description])}")
                df = pd.DataFrame.from_records(records, columns=columns)
                cursor.close()
                return df
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logger.error(f"Error executing query on {self.server}/{self.database}: {sqlstate}")
            logger.error(ex)
            return None

    def execute_non_query(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        """Execute DDL/DML (e.g., TRUNCATE, ALTER INDEX, INSERT/UPDATE/DELETE) and return affected rows."""
        conn = None
        attempts = 0
        max_attempts = 5
        base_sleep = 0.25  # seconds
        while True:
            try:
                conn = self.connect()
                cur = conn.cursor()
                if params is None:
                    cur.execute(sql)
                else:
                    normalized = self._normalize_params(params)
                    cur.execute(sql, normalized)
                affected = cur.rowcount
                # Some drivers/settings (e.g., NOCOUNT ON, certain RPC paths) return -1.
                # Fallback to @@ROWCOUNT which reliably returns rows affected by the last statement.
                if affected == -1:
                    try:
                        cur.execute("SELECT @@ROWCOUNT")
                        res = cur.fetchone()
                        affected = int(res[0]) if res is not None else -1
                    except Exception:
                        pass
                # logger.info(f"Executed non query affected rows is : {affected}")
                if not self.autocommit:
                    conn.commit()
                cur.close()
                return affected
            # except Exception as e:
            #     sqlstate = e.args[0]
            #     logger.error(f"Error executing query on {self.server}/{self.database}: {sqlstate}")
            #     logger.info(f"params are : {params}")
            #     exit()
            #     return None
            # finally:
            #     try:
            #         conn.close()
            #     except Exception:
            #         pass
            except pyodbc.Error as e:
                # SQLSTATE '40001' = serialization/deadlock; 1205 is SQL Server deadlock victim
                sqlstate = e.args[0] if e.args else None
                errnum = getattr(e, 'args', [None, None])[1]
                if (sqlstate == '40001' or errnum == 1205) and attempts < max_attempts - 1:
                    attempts += 1
                    sleep = base_sleep * (2 ** (attempts - 1)) + random.uniform(0, 0.2)
                    logger.warning(f"Deadlock detected (attempt {attempts}/{max_attempts}). Retrying in {sleep:.2f}s...")
                    try:
                        if conn:
                            conn.close()
                    except Exception:
                        pass
                    time.sleep(sleep)
                    continue
                logger.error(f"Error executing query on {self.server}/{self.database}: {e}")
                logger.info(f"params are : {params}")
                raise
            finally:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass

    def executemany(self, sql: str, params: Sequence[Sequence[Any]]) -> int:
        """Bulk execute parameterized INSERT/UPDATE/DELETE using executemany; returns total affected rows."""
        conn = None
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.fast_executemany = True
            # Normalize params: convert NaN/NaT to None (NULL), unwrap numpy scalars to Python types
            normalized_params: list[tuple[Any, ...]] = [
                self._normalize_param_row(row) for row in params
            ]

            cur.executemany(sql, normalized_params)
            rows_inserted = cur.rowcount
            # With fast_executemany and some drivers, rowcount can be -1 despite success.
            # If no exception was raised, assume all parameter rows were applied.
            if rows_inserted == -1:
                rows_inserted = len(normalized_params)
            logger.info(f"Executed executemany affected rows is : {rows_inserted}")
            if not self.autocommit:
                conn.commit()
            cur.close()
            return rows_inserted
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _normalize_param_row(self, row: Sequence[Any]) -> tuple[Any, ...]:
        """Convert a single parameter row to DB-friendly Python types (NaN/NaT->None, numpy->native)."""
        normalized_row = []
        for value in row:
            normalized_row.append(self._normalize_value(value))
        return tuple(normalized_row)

    def _normalize_value(self, value: Any) -> Any:
        """Normalize a single value for pyodbc: NaN/NaT->None, numpy->native, pandas Timestamp->datetime."""
        # None stays None
        if value is None:
            return None
        # Pandas NA/NaT
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        # Numpy scalars -> native Python
        try:
            import numpy as np  # local import
            if isinstance(value, (np.generic,)):
                return value.item()
            # numpy datetime64 -> python datetime
            if isinstance(value, (np.datetime64,)):
                return pd.Timestamp(value).to_pydatetime()
        except Exception:
            pass
        # Pandas Timestamp -> python datetime
        try:
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
        except Exception:
            pass
        return value

    def _normalize_params(self, params: Sequence[Any] | Mapping[str, Any] | Any) -> tuple[Any, ...] | dict[str, Any]:
        """Normalize params passed to execute(): supports dict, sequence, or scalar -> tuple."""
        # Mapping (named parameters)
        try:
            if isinstance(params, Mapping):
                out: dict[str, Any] = {}
                for k, v in params.items():
                    out[k] = self._normalize_value(v)
                return out
        except Exception:
            pass
        # Treat as sequence if it's iterable and not a string/bytes/bytearray
        try:
            is_iterable = iter(params) is not None  # type: ignore[arg-type]
        except TypeError:
            is_iterable = False
        if is_iterable and not isinstance(params, (str, bytes, bytearray)):
            return tuple(self._normalize_value(v) for v in params)  # type: ignore[iteration-over-optional]
        # Scalar -> single-element tuple
        return (self._normalize_value(params),)

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info(f"Connection to {self.server}/{self.database} closed.")
    
    @staticmethod
    def _to_bool(val):
        if isinstance(val, bool):
            return val
        if val is None:
            return False
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            return val.strip().lower() in ("1", "true", "t", "yes", "y", "on")
        return bool(val)