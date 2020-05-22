import psycopg2
from pglast.enums import ObjectType, AlterTableType
from pglast.printer import IndentedStream
import multiprocessing
import subprocess
import logging


def copy_table(source_dsn, target_dsn, source_table, target_table,
               snapshot_name=None, include_inherited=False):
    copy_script = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    if snapshot_name is not None:
        copy_script += "SET TRANSACTION SNAPSHOT '%s';" % snapshot_name
    if include_inherited:
        copy_script += "COPY (SELECT * FROM %s) TO STDOUT;" % source_table
    else:
        copy_script += "COPY %s TO STDOUT;" % source_table
    pout = subprocess.Popen(["psql", "-d", source_dsn, "-X",
                            "-c", copy_script],
                            stdout=subprocess.PIPE)
    pin = subprocess.Popen(["psql", "-d", target_dsn,
                            "-X", "-c", "COPY %s FROM STDIN" % target_table],
                           stdin=pout.stdout)
    pin.wait()




def restore_tables(source_dsn, target_dsn, table_mapping, njobs=4,
                   snapshot_name=None, include_inherited=False):
    pool = multiprocessing.Pool(njobs)
    for source_table, target_table in table_mapping.items():
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                         target_table, snapshot_name, include_inherited))
    pool.close()
    pool.join()
