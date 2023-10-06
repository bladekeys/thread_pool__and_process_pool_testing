from datetime import datetime, timedelta
import time
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import os
import pandas as pd
from sqlalchemy import create_engine

server = ''
username = 'sa'
root_folder = os.path.dirname(os.path.realpath(__file__))
with open("{}/psswrd.txt".format(root_folder), 'r') as f:
    db_password, email_password = f.readlines()
    db_password = db_password.rstrip()

params = urllib.parse.quote_plus(
    f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};UID={username};PWD={db_password};Mars_Connection=Yes")

SQLALCHEMY_DATABASE_URI = "mssql+pyodbc:///?odbc_connect=%s" % params

mydb = create_engine(SQLALCHEMY_DATABASE_URI)


class Threads:
    def __init__(self):
        self.pool = ThreadPoolExecutor()

    def read_queries(self, queries, db, index_col=None):
        task = [self.pool.submit(self._read_sql, i, query, db, index_col)
                for i, query in enumerate(queries)]

        return dict(res.result() for res in as_completed(task))

    @staticmethod
    def _read_sql(i, query, db, index_col):
        return i, pd.read_sql(query, db, index_col)


class Process:
    def __init__(self):
        self.pool = ProcessPoolExecutor()

    def read_queries(self, queries, index_col=None):
        task = [self.pool.submit(self._read_sql, i, query, index_col)
                for i, query in enumerate(queries)]

        return dict(res.result() for res in as_completed(task))

    @staticmethod
    def _read_sql(i, query, index_col):
        return i, pd.read_sql(query, mydb, index_col)


threads = Threads()

processes = Process()


def pool_process(From, to):
    to_date = datetime.strptime(to, '%Y-%m-%d')
    to_date += timedelta(days=1)
    start = time.time()
    processes_result = processes.read_queries(
        ("SELECT [DT], [BB19], [BB42], [BB65], [BB88], [BB1], [BB24], [BB47], [BB70]"
         " FROM [VRKH5].[dbo].[DGU] WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' "
         "ORDER BY [DT] ASC".format(From, to),
         "SELECT [DT], [AA116], [AA24], [AA47], [AA19], [AA20], [AA42], [AA65], [AA88], [AA89], [AA111],"
         " [AA112], [AA134], [AA135], [AA157], [AA158] FROM [VRKH5].[dbo].[CNE_CEC] WHERE CAST([DT] as Date)"
         " BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(From, to),
         "SELECT [DT], [CC3], [CC10], [CC11], [CC16], [CC23], [CC24], [CC27], [CC34], [CC35], [CC38], [CC45], "
         "[CC46], [CC49], [CC56], [CC57], [CC60], [CC67], [CC68], [CC71], [CC78], [CC79], [CC1], [CC6], [CC14], "
         "[CC25], [CC36], [CC47], [CC58], [CC69], [CC19], [CC30], [CC41], [CC52], [CC63], [CC74] FROM [VRKH5].["
         "dbo].[CEC_INVERTOR] WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
             From, to),
         "SELECT [DT], [DD1], [DD2], [DD3], [DD4], [DD5], [DD18], [DD15], [DD16], [DD17], [DD19]"
         "  FROM [VRKH5].[dbo].[CNE_INVERTOR] WHERE [DT] BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
             From, to_date),
         "SELECT [DT], [F64], [F65], [F68], [F69], [F1], [F2], [F3], [F4], [F27], [F28], [F29], [F30], "
         "[F31], [F32], [F33] from [VRKH1].[dbo].[Table2] WHERE [DT] BETWEEN '" +
         From + "' AND '" + to + "' ORDER BY [DT] ASC;"), index_col='DT')
    dgu_gen = processes_result[0]
    energy_meters = processes_result[1]
    inverters = processes_result[2]
    battery_data = processes_result[3]
    minute_data24 = processes_result[4]
    print(f"Многопроцессорно {time.time() - start}")


def pool_thread(From, to):
    to_date = datetime.strptime(to, '%Y-%m-%d')
    to_date += timedelta(days=1)
    start = time.time()
    threads_result = threads.read_queries(
        ("SELECT [DT], [BB19], [BB42], [BB65], [BB88], [BB1], [BB24], [BB47], [BB70]"
         " FROM [VRKH5].[dbo].[DGU] WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' "
         "ORDER BY [DT] ASC".format(From, to),
         "SELECT [DT], [AA116], [AA24], [AA47], [AA19], [AA20], [AA42], [AA65], [AA88], [AA89], [AA111],"
         " [AA112], [AA134], [AA135], [AA157], [AA158] FROM [VRKH5].[dbo].[CNE_CEC] WHERE CAST([DT] as Date)"
         " BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(From, to),
         "SELECT [DT], [CC3], [CC10], [CC11], [CC16], [CC23], [CC24], [CC27], [CC34], [CC35], [CC38], [CC45], "
         "[CC46], [CC49], [CC56], [CC57], [CC60], [CC67], [CC68], [CC71], [CC78], [CC79], [CC1], [CC6], [CC14], "
         "[CC25], [CC36], [CC47], [CC58], [CC69], [CC19], [CC30], [CC41], [CC52], [CC63], [CC74] FROM [VRKH5].["
         "dbo].[CEC_INVERTOR] WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
             From, to),
         "SELECT [DT], [DD1], [DD2], [DD3], [DD4], [DD5], [DD18], [DD15], [DD16], [DD17], [DD19]"
         "  FROM [VRKH5].[dbo].[CNE_INVERTOR] WHERE [DT] BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
             From, to_date),
         "SELECT [DT], [F64], [F65], [F68], [F69], [F1], [F2], [F3], [F4], [F27], [F28], [F29], [F30], "
         "[F31], [F32], [F33] from [VRKH1].[dbo].[Table2] WHERE [DT] BETWEEN '" +
         From + "' AND '" + to + "' ORDER BY [DT] ASC;"),
        mydb, index_col='DT')
    dgu_gen = threads_result[0]
    energy_meters = threads_result[1]
    inverters = threads_result[2]
    battery_data = threads_result[3]
    minute_data24 = threads_result[4]
    print(f"Многопоточно {time.time() - start}")


def normal(From, to):
    to_date = datetime.strptime(to, '%Y-%m-%d')
    to_date += timedelta(days=1)
    start = time.time()
    dgu_gen = pd.read_sql(
        "SELECT [DT], [BB19], [BB42], [BB65], [BB88], [BB1], [BB24], [BB47], [BB70] FROM [VRKH5].[dbo].[DGU]"
        "WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
            From, to),
        mydb, index_col='DT')

    energy_meters = pd.read_sql(
        "SELECT [DT], [AA116], [AA24], [AA47], [AA19], [AA20], [AA42], [AA65], [AA88], [AA89], [AA111],"
        " [AA112], [AA134], [AA135], [AA157], [AA158] FROM [VRKH5].[dbo].[CNE_CEC] WHERE CAST([DT] as Date)"
        " BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
            From, to),
        mydb, index_col='DT')

    inverters = pd.read_sql(
        "SELECT [DT], [CC3], [CC10], [CC11], [CC16], [CC23], [CC24], [CC27], [CC34], [CC35], [CC38], [CC45], "
        "[CC46], [CC49], [CC56], [CC57], [CC60], [CC67], [CC68], [CC71], [CC78], [CC79], [CC1], [CC6], [CC14], "
        "[CC25], [CC36], [CC47], [CC58], [CC69], [CC19], [CC30], [CC41], [CC52], [CC63], [CC74] FROM [VRKH5].["
        "dbo].[CEC_INVERTOR] WHERE CAST([DT] as Date) BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
            From, to),
        mydb, index_col='DT')

    battery_data = pd.read_sql(
        "SELECT [DT], [DD1], [DD2], [DD3], [DD4], [DD5], [DD18], [DD15], [DD16], [DD17], [DD19]"
        "  FROM [VRKH5].[dbo].[CNE_INVERTOR] WHERE [DT] BETWEEN '{}' AND '{}' ORDER BY [DT] ASC".format(
            From, to_date),
        mydb, index_col='DT')
    minute_data24 = pd.read_sql_query(
        "SELECT [DT], [F64], [F65], [F68], [F69], [F1], [F2], [F3], [F4], [F27], [F28], [F29], [F30], "
        "[F31], [F32], [F33] from [VRKH1].[dbo].[Table2] WHERE [DT] BETWEEN '" +
        From + "' AND '" + to + "' ORDER BY [DT] ASC;",
        mydb)
    print(f"Последовательно {time.time() - start}")


if __name__ == '__main__':
    for i in ('2023-10-01', '2023-09-02', '2023-07-02'):
        for j in ('2023-10-05', '2023-10-06'):
            normal(i, j)
            pool_thread(i, j)
            pool_process(i, j)
