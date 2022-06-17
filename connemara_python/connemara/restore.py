import psycopg2
from pglast.enums import ObjectType, AlterTableType
from pglast.printer import IndentedStream
import multiprocessing
import subprocess
import logging
import os

def copy_table(source_dsn, target_dsn, source_table, target_table,
               snapshot_name=None, include_inherited=False,
               nb=0, part=1):
    copy_script = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    if snapshot_name is not None:
        copy_script += "SET TRANSACTION SNAPSHOT '%s';" % snapshot_name
    if include_inherited:
        copy_script += "COPY (SELECT * FROM %s) TO STDOUT;" % source_table
    if part > 1:
        copy_script += "COPY (SELECT * FROM %s WHERE (ctid::text::point)[0]::int % %s = %s) TO STDOUT;" % (source_table, part, nb)
    else:
        copy_script += "COPY %s TO STDOUT;" % source_table
    logger = multiprocessing.get_logger()
    try:
        pout = subprocess.Popen(["psql", "-d", source_dsn, "-X",
                                "-c", copy_script],
                                stdout=subprocess.PIPE)
        pin = subprocess.Popen(["psql", "-d", target_dsn,
                                "-X", "-c", "COPY %s FROM STDIN" % target_table],
                                stdin=pout.stdout)
        pin.wait()

        pout.wait()
        status = pout.returncode
        if status != 0:
            logger.info("retcode = %s killing...." % status)
            logger.info("pid = %s" % os.getpid())
            logger.info("ppid = %s" % os.getppid())
            os.system('kill -9 {0}'.format(os.getppid()))
            sys.exit(99)
    except:
        logger.info("Error > %s" % sys.exc_info()[0])



def restore_tables(source_dsn, target_dsn, table_mapping, njobs=4,
                   snapshot_name=None, include_inherited=False):
    multiprocessing.log_to_stderr()
    pool = multiprocessing.Pool(njobs)
    for source_table, target_table in table_mapping.items():
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 0, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 1, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 2, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 3, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 4, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 5, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 6, 8))
        pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                     target_table, snapshot_name, include_inherited, 7, 8))
    pool.close()
    pool.join()
