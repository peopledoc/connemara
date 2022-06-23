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
    logger = multiprocessing.get_logger()
    copy_script = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    if snapshot_name is not None:
        copy_script += "SET TRANSACTION SNAPSHOT '%s';" % snapshot_name
    if include_inherited:
        copy_script += "COPY (SELECT * FROM %s) TO STDOUT;" % source_table
    if part > 1:
        copy_script += "COPY (SELECT * FROM {0} WHERE (ctid::text::point)[0]::int % {1} = {2}) TO STDOUT;".format(source_table, part, nb)
    else:
        copy_script += "COPY %s TO STDOUT;" % source_table
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
        logger.exception("Error > %s" % sys.exc_info()[0])

def restore_tables(source_dsn, target_dsn, table_mapping, njobs=4,
                   snapshot_name=None, include_inherited=False):
    logger = logging.getLogger()
    logger.info("restore_tables start")
    multiprocessing.log_to_stderr()
    pool = multiprocessing.Pool(njobs)
    for source_table, target_table in table_mapping.items():
        for i in range (0, njobs):
            pool.apply_async(copy_table, (source_dsn, target_dsn, source_table,
                             target_table, snapshot_name, include_inherited, i, njobs))
    pool.close()
    pool.join()
    logger.info("restore_tables end")
