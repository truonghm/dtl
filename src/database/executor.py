import sqlite3
import sqlalchemy
import pandas as pd
import numpy as np
import warnings
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(sys.path[0])))

from src.config import Setting


class SQLiteDatabase:
    def __init__(self):
        try:
            self.connection = sqlite3.connect(Setting.DATABASE)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(repr(e))

    def close(self):
        self.connection.close()

    def select(self, query):
        return pd.read_sql(query, self.connection)

    def execute(self, query):
        try:
            print(query)
            result = self.cursor.execute(query)
            self.connection.commit()
            print("excecute done")
            return result
        except sqlite3.Error as e:
            (error_msg,) = e.args
            print("Database Error:", error_msg)
            # print(e, query, values)
            return

    def truncate_table(self, table_name):
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
        except sqlite3.Error as e:
            (error_msg,) = e.args
            print("Database Error:", error_msg)

    def drop_table(self, table_name):
        try:
            self.cursor.execute(f"DROP TABLE {table_name}")
        except sqlite3.Error as e:
            (error_msg,) = e.args
            print("Database Error:", error_msg)

    def mapping_dtype(self, dtype):
        if dtype.lower() == "int64":
            return " INT"
        elif dtype.lower() == "float64":
            return " FLOAT"
        elif dtype.lower() == "object":
            return " varchar2(255)"
        else:
            return " varchar2(255)"

    def bulk_insert(
        self,
        df,
        table_name: str,
        if_exists="replace",
        fill_nan: bool = True,
        fill_inf: bool = False,
        convert_to_str: bool = False,
    ):

        print("METHOD OF BULK INSERT IS: ", if_exists)

        new_df = df.copy(deep=True)

        invalid_dtypes = []
        mapping_dtypes = []

        for col, col_type in df.dtypes.items():
            if col_type not in ["int64", "float64", "object", "datetime64[ns]"]:
                invalid_dtypes.append(col)
                if convert_to_str:
                    new_df[col] = new_df[col].astype(str)

            if col_type != "datetime64[ns]":
                mapping_dtypes.append(f"{col} {self.mapping_dtype((str(col_type)))}")
            else:
                mapping_dtypes.append(f"{col} TEXT")

            if col_type == "object":
                new_df[col] = new_df[col].replace(np.nan, None)
            elif col_type in ["int64", "float64"]:
                replace_vals = []
                if fill_nan:
                    replace_vals.append(np.nan)
                if fill_inf:
                    replace_vals.extend([np.inf, -np.inf])
                new_df[col] = np.where(
                    new_df[col].isin(replace_vals), None, new_df[col]
                )
        # invalid_dtypes = [k for k,v in new_df.dtypes.items() if v not in ["int64", "float64", "object", "datetime64[ns]"]]
        print("invalid dtypes:", invalid_dtypes)

        if len(invalid_dtypes) > 0:
            if convert_to_str:
                warnings.warn(
                    f"Column(s) [{','.join(invalid_dtypes)}] have dtypes not in ['int64','float64','object','datetime64[ns]'] and will be converted to 'object'"
                )
            else:
                raise Exception(
                    f"Column(s) [{','.join(invalid_dtypes)}] have invalid dtypes (must be one of ['int64','float64','object','datetime64[ns]']"
                )

        try:
            # connection = self.pool.acquire()
            # cursor = connection.cursor()
            if if_exists not in ("replace", "append", "truncate"):
                raise Exception("Invalid input for `if_exists`")
            else:
                # dtype_mapping = [f"{k} {self.mapping_type((str(v)))}" if str(v) != "datetime64[ns]" else f"{k} TIMESTAMP" for k,v in df.dtypes.items()]
                create_table_statement = f"CREATE TABLE {table_name} ( "
                create_table_statement += ",".join(mapping_dtypes)
                create_table_statement += " )"

                try:
                    self.cursor.execute(f"SELECT 1 from {table_name}")
                    self.connection.commit()
                    exist_status = True
                except:
                    exist_status = False

                if not exist_status:
                    try:
                        print(create_table_statement)
                        self.cursor.execute(create_table_statement)
                        self.connection.commit()
                    except sqlite3.Error as e:
                        (error,) = e.args
                        print("Database Error: ", error)
                        raise (e)

                elif exist_status and if_exists == "replace":
                    try:
                        drop_table_statement = f"DROP TABLE {table_name}"
                        print(drop_table_statement)
                        self.cursor.execute(drop_table_statement)
                        self.connection.commit()

                        print(create_table_statement)
                        self.cursor.execute(create_table_statement)
                        self.connection.commit()
                    except sqlite3.Error as e:
                        (error,) = e.args
                        print("Database Error: ", error)
                        raise (e)

                if if_exists == "truncate":
                    try:
                        trunc_table_statement = f"TRUNCATE TABLE {table_name}"
                        self.cursor.execute(trunc_table_statement)
                        self.connection.commit()
                    except sqlite3.Error as e:
                        (error,) = e.args
                        print("Database Error: ", error)
                        raise (e)

            date_cols = []
            for k, v in new_df.dtypes.items():
                if v == "datetime64[ns]":
                    new_df[k] = new_df[k].dt.strftime("%Y-%m-%d %H:%M:%S")
                    date_cols.append(k)

            col_query_list = []
            for index, col in enumerate(new_df.columns):
                sql_index = index + 1
                if col in date_cols:
                    # col_str = f"TO_DATE( :{sql_index}, 'YYYY-MM-DD HH24:mi:SS')"
                    col_str = f":{sql_index}"
                else:
                    col_str = f":{sql_index}"
                col_query_list.append(col_str)

            insert_query_cols = ",".join(col_query_list)

            insert_query = (
                f""" INSERT INTO {table_name.lower()} VALUES ({insert_query_cols} )"""
            )

            df_values = [tuple(x) for x in new_df.values]
            print(insert_query)

            try:
                self.cursor.executemany(insert_query, df_values)
                self.connection.commit()
                print(f"Bulk insert done with method {if_exists}")
            except sqlite3.Error as e:
                (error,) = e.args
                print("Database Error: ", error)
                raise (e)
        finally:
            # self.pool.release(connection)
            pass

    def insert(
        self, dataframe: pd.DataFrame, table_name: str, if_exists: str = "replace"
    ):
        pass
