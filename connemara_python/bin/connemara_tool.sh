#!/bin/bash

#set -x

line () {
  echo '-----------------------------------------------------------------------------'
}

help () {
  echo
  echo "Usage: su -m connemara -c '$(basename ${CMD}) action /etc/connemara_replication/xxx.env'"
  echo '  with action in:'
  echo '    slot_clean   drop replication slots and als'
  echo '    data_backup  rename schemas and tables'
  echo '    data_drop    drop schemas and tables'
  echo '    basebackup   run basebackup'
  echo '    grant        fix schema and tables privileges'
  echo '  with config file in:'
  (cd /etc/connemara_replication/; ls *.env) #| sed -e 's/^/    /'
  echo
}

is_in_list()
{
  local s="${3-" "}";
  case "$s$2$s" in (*"$s$1$s"*) return 0;; esac; return 1;
}

CMD=$0
ACTION=$1
if ! is_in_list "${ACTION}" "slot_clean data_backup data_drop basebackup grant help"; then
  echo "Error: action ${ACTION} unknown"
  help
  exit 1
fi

CONF=$2
CONFBASE=$(basename "${CONF}")
if [ "${CONF}" == "" ] ; then
  echo "Error: config file ${CONF} undefined"
  help
  exit 2
fi
if [ ! -f "${CONF}" ] ; then
  echo "Error: unexistent env file ${CONF}"
  help
  exit 3
fi
if [ ! -r "${CONF}" ] ; then
  echo "Error: unreadable env file ${CONF}"
  id
  ls -l "${CONF}"
  help
  exit 3
fi

# Load config
. "${CONF}"
CONFNAME="${CONFBASE%.*}"
DB=$(echo "${CONNEMARA_SOURCE_DSN}" | grep -oE 'dbname=[^= ]+' | cut -d= -f2)
USERNAME=$(id -nu)

# Display config
line
date
echo "CMD                  ${CMD}"
echo "CONF                 ${CONF}"
echo "CONFBASE             ${CONFBASE}"
echo "CONFNAME             ${CONFNAME}"
echo "CONNEMARA_SOURCE_DSN ${CONNEMARA_SOURCE_DSN}"
echo "CONNEMARA_TARGET_DSN ${CONNEMARA_TARGET_DSN}"
echo "CONNEMARA_SLOT       ${CONNEMARA_SLOT}"
echo "DB (DATABASE)        ${DB}"
echo "USERNAME             ${USERNAME}"
line

# Check config
if [ "${CONNEMARA_SOURCE_DSN}" == "" ] ; then
  echo "Error: CONNEMARA_SOURCE_DSN undefined"
  help
  exit 4
fi
if [ "${CONNEMARA_TARGET_DSN}" == "" ] ; then
  echo "Error: CONNEMARA_TARGET_DSN undefined"
  help
  exit 5
fi
if [ "${CONNEMARA_SLOT}" == "" ] ; then
  echo "Error: CONNEMARA_SLOT undefined"
  help
  exit 6
fi
if [ "${USERNAME}" != "connemara" ] ; then
  echo "Error: should be run by connemara"
  help
  exit 7
fi

# Config validated, then run action
# FIXME: Add "Are you sure?"
date
echo "${ACTION} starts"
line

# slot_clean will drop all replication slots and origins on source and target
#            connemara replication must be stopped for this source
if [ "${ACTION}" == "help" ] ; then
  #FIXME: Replace help by a detailed step by step process
  help
  exit 0
elif [ "${ACTION}" == "slot_clean" ] ; then
  # Drop old slot
  DAEMONSTATE=$(systemctl status connemara_replication@${CONFNAME}.service 2>&1 >/dev/null; echo $?)
  if [ "${DAEMONSTATE}" -eq 0 ] ; then
    echo "DAEMONSTATE          ${DAEMONSTATE}"
    echo 'Connemara replication must be stop. Please execute command like:'
    echo "sudo systemctl stop connemara_replication@${CONFNAME}.service"
    exit 8
  fi
  echo 'Cleaning source'
  psql "${CONNEMARA_SOURCE_DSN}" -Xa -c "SELECT pg_drop_replication_slot('${CONNEMARA_SLOT}')"
  psql "${CONNEMARA_SOURCE_DSN}" -Xa -c "SELECT pg_replication_origin_drop('${CONNEMARA_SLOT}')"
  echo
  echo 'Cleaning target'
  psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SELECT pg_replication_origin_drop('${CONNEMARA_SLOT}')"
  # || exit 1
  #line
  #date
  #echo 'Existing replication slots'
  #psql "${CONNEMARA_SOURCE_DSN}" -Xa -c "SELECT slot_name, database FROM pg_replication_slots"
  #line
  #date
  #echo 'Stats replication.raw_messages'
  #psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SET ROLE postgres; SELECT database, source_slotname, count(*), min(insert_timestamp), max(insert_timestamp) FROM replication.raw_messages GROUP BY 1,2 ORDER BY 1,2;"
  echo
  echo "Deleting source from replication.raw_messages"
  psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SET ROLE postgres; DELETE FROM replication.raw_messages WHERE source_slotname = '${CONNEMARA_SLOT}';" || exit 9

# data_backup will rename schema with timestamp keeping data for later analysis
elif [ "${ACTION}" == "data_backup" ] ; then
  TS=$(date -u +%Y%m%d_%H%M%S)
  echo "Backup schema ${DB}_public to ${DB}_public_${TS}"
  psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SET ROLE postgres; ALTER SCHEMA ${DB}_public RENAME TO ${DB}_public_${TS};"
  # FIXME: only app_public is backup

# data_drop will drop schema
elif [ "${ACTION}" == "data_drop" ] ; then
  echo "Drop schema ${DB}_public"
  psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SET ROLE postgres; DROP SCHEMA ${DB}_public CASCADE;" 
  # FIXME: only app_public is dropped

# basebackup creates slot, origins, takes basebackup, rebuilds index
# FIXME: On source (narcisse): ALTER ROLE replicator NOSUPERUSER;
elif [ "${ACTION}" == "basebackup" ] ; then
  echo 'Basebackup'
  echo 'Connemara replication slot must be absent'
  psql "${CONNEMARA_SOURCE_DSN}" -Xa -c "SELECT slot_name, database FROM pg_replication_slots WHERE slot_name = '${CONNEMARA_SLOT}'"
  echo
  echo 'Connemara replication slot must be absent'
  psql "${CONNEMARA_SOURCE_DSN}" -Xa -c "SELECT * FROM pg_replication_origin WHERE roname = '${CONNEMARA_SLOT}'"
  psql "${CONNEMARA_TARGET_DSN}" -Xa -c "SELECT * FROM pg_replication_origin WHERE roname = '${CONNEMARA_SLOT}'"
  echo
  connemara_basebackup.py -d \
    --source "${CONNEMARA_SOURCE_DSN}" \
    --target "${CONNEMARA_TARGET_DSN}" \
    --slot   "${CONNEMARA_SLOT}"

# grant fix privileges
elif [ "${ACTION}" == "grant" ] ; then
  echo "Fixing privileges"
#    --REVOKE ALL   ON SCHEMA :s FROM biologic_views_builder; \
#    --REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s TO   biologic_views_builder; \
#    --REVOKE ALL   ON SCHEMA :s FROM datapenade; \
#    --REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s TO   datapenade; \
#    --nvexport_legacy \
#    --superset_rw \
#    GRANT  ALL ON FUNCTION public.lookup TO pgbouncer; \
  echo " \
    SET ROLE postgres; \
    \
    GRANT kettle,biologic_views_builder TO dba; \
    \
    REVOKE ALL   ON SCHEMA :s FROM biologic_ro,biologic_rw; \
    GRANT  USAGE ON SCHEMA :s TO   biologic_ro,biologic_rw; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM biologic_ro,biologic_rw; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   biologic_ro,biologic_rw; \
    GRANT  INSERT,UPDATE,DELETE ON ALL TABLES    IN SCHEMA :s TO   biologic_rw; \
    GRANT  ALL                  ON ALL SEQUENCES IN SCHEMA :s TO   biologic_rw; \
    GRANT  ALL                  ON ALL FUNCTIONS IN SCHEMA :s TO   biologic_rw; \
    \
    GRANT  USAGE ON SCHEMA :s TO   biologic_views_builder; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   biologic_views_builder; \
    \
    GRANT  USAGE ON SCHEMA :s TO   datapenade; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   datapenade; \
    \
    REVOKE ALL   ON SCHEMA :s FROM export_runner; \
    GRANT  USAGE ON SCHEMA :s TO   export_runner; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM export_runner; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   export_runner; \
    \
    GRANT  USAGE ON SCHEMA :s TO   kettle; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   kettle; \
    \
    REVOKE ALL   ON SCHEMA :s FROM looker_int; \
    GRANT  USAGE ON SCHEMA :s TO   looker_int; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM looker_int; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   looker_int; \
    \
    REVOKE ALL   ON SCHEMA :s FROM looker; \
    GRANT  USAGE ON SCHEMA :s TO   looker; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM looker; \
    GRANT  SELECT               ON ALL TABLES    IN SCHEMA :s TO   looker; \
    \
    REVOKE ALL   ON SCHEMA :s FROM pgbouncer; \
    \
    REVOKE ALL   ON SCHEMA :s FROM dba; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM dba; \
    REVOKE ALL                  ON ALL SEQUENCES IN SCHEMA :s FROM dba; \
    REVOKE ALL                  ON ALL FUNCTIONS IN SCHEMA :s FROM dba; \
    \
    REVOKE ALL   ON SCHEMA :s FROM pgbouncer; \
    REVOKE ALL                  ON ALL TABLES    IN SCHEMA :s FROM pgbouncer; \
    \
    REVOKE ALL   ON SCHEMA public FROM public; \
    GRANT  USAGE ON SCHEMA public TO   public; \
    REVOKE ALL ON ALL TABLES IN SCHEMA public FROM public; \
  " | psql "${CONNEMARA_TARGET_DSN}" -X "-vs=${DB}_public"
else
  echo "Error: unimplemented action ${ACTION}"
  exit 99
fi

line
date
echo "${ACTION} done"
line
exit 0
