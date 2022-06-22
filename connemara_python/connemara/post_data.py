import psycopg2
from pglast.enums import ObjectType, AlterTableType, ConstrType
from pglast.printer import IndentedStream
import multiprocessing
import logging
import subprocess
import logging
import os

def should_apply_post_data_stmt(stmt):
    if stmt.node_tag == 'AlterTableStmt':
        # In case of multiple commands, be safe and apply it
        if len(stmt.cmds) != 1:
            return True
        cmd = stmt.cmds[0]
        # Only keep some types of constraints
        if cmd.subtype == AlterTableType.AT_AddConstraint:
            kept_consttypes = (
                    ConstrType.CONSTR_PRIMARY,
                    ConstrType.CONSTR_UNIQUE,
                    ConstrType.CONSTR_FOREIGN)
            return getattr(cmd, 'def').contype.value in kept_consttypes
        if cmd.subtype == AlterTableType.AT_ClusterOn:
            return False
        return True
    if stmt.node_tag == 'IndexStmt':
        if stmt.unique:
            return True
        return False
    # When in doubt, keep it
    return True

def post_data_task(target_dsn, stmt):
    try:
        logger = multiprocessing.get_logger()
        logger.info(stmt)
        pin = subprocess.Popen(["psql", "-d", target_dsn, "-X", "-c", stmt])
        pin.wait()
        status = pin.returncode
        if status != 0:
            logger.info("retcode = %s killing...." % status)
            logger.info("pid = %s" % os.getpid())
            logger.info("ppid = %s" % os.getppid())
            os.system('kill -9 {0}'.format(os.getppid()))
            sys.exit(99)
    except:
        logger.exception("Error > %s" % sys.exc_info()[0])

def post_data_exec(target_dsn, statements, njobs=4):
    logging.getLogger().info("post_data_exec start")
    multiprocessing.log_to_stderr()
    pool2 = multiprocessing.Pool(njobs)
    j = 0
    for stmt in statements:
        if should_apply_post_data_stmt(stmt):
            pool2.apply_async(post_data_task, (target_dsn, IndentedStream(expression_level=1)(stmt)))
            j = j+1
    logging.getLogger().info("post_data_exec: jobs={0}, stmts={1}".format(njobs,j))
    pool2.close()
    pool2.join()
    logging.getLogger().info("post_data_exec end")
