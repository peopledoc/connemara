#!/bin/bash

JOB=16
PSQL='psql -X'
DSN=$1
if [ "$DSN" == "" ] ; then
  echo 'Usage: connemara_reindex.bash "dbname=xxx port=5432"'
  exit 1
fi


### Drop invalid index ###

QUERY="SELECT 'DROP INDEX CONCURRENTLY ' || indexrelid::regclass \
  FROM pg_index \
 WHERE NOT indisvalid \
   AND NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation=pg_index.indexrelid) \
ORDER BY random()"

echo 'Drop INVALID indexes'
$PSQL "$DSN" -Atc "$QUERY" | xargs -I{} -P$JOB sh -c "echo '{}'; echo 'SET ROLE postgres; {};' | $PSQL '$DSN' -At"


### Create missing index on FK ###

# Without CONCURENTLY, with timeout = 0
# Force rebuild in //
# May crash connemara replication but faster

# We need to limit ourselves to indnkeyatts because the keys after that aren't indexed
# then to array_upper(conkey,1) because our index must START exactly the same as the conkey
# the slice on indkey is offset by -1 because indkey is a int2vector, those start at 0
# As the key order is of no importance, the arrays just need to be included in each other
# (equals in the ensemblist sense)
QUERY="WITH not_indexed_constraints AS ( \
  SELECT conname, conrelid::regclass AS tablename, conkey \
    FROM pg_constraint \
   WHERE contype = 'f' \
     AND NOT EXISTS ( \
           SELECT 1 \
             FROM pg_index \
            WHERE indrelid=conrelid \
              AND ((indkey::int4[])[0:indnkeyatts-1])[1:array_upper(conkey,1)]@>conkey::int4[] \
              AND ((indkey::int4[])[0:indnkeyatts-1])[1:array_upper(conkey,1)]<@conkey::int4[] \
         ) \
 ), unnested_constraints AS ( \
  SELECT conname, tablename, unnest.* \
    FROM not_indexed_constraints,unnest(conkey) with ordinality \
 ) \
SELECT 'CREATE INDEX ON ' || tablename::text || '(' || \
       string_agg(quote_ident(attname::text), ',' order by ordinality) || ')' \
  FROM unnested_constraints \
  JOIN pg_attribute ON unnested_constraints.tablename=pg_attribute.attrelid \
                   AND pg_attribute.attnum=unnested_constraints.unnest \
GROUP BY tablename, conname \
ORDER BY tablename, random()"

echo 'Create missing FOREIGN KEY indexes'
$PSQL "$DSN" -Atc "$QUERY" | xargs -I{} -P$JOB sh -c "echo '{}'; echo 'SET ROLE postgres; SET statement_timeout TO 0; SET lock_timeout TO 0; {};' | $PSQL '$DSN' -At"
