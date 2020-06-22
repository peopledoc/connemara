/*
 * Copyright (c) 2018 PeopleDoc <dba@people-doc.com >
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include "connemara_replication.h"

#include <arpa/inet.h>
#include <pcre.h>
#include <time.h>
#include <sys/time.h>

volatile sig_atomic_t time_to_abort = false;

static int64_t standby_message_timeout = 10 * 1000;
static int64_t log_position_timeout = 10 * 60 * 1000 * 1000;	/* Log every 10 minutes */
static int64_t last_logged_position;
static int64_t now;

static pcre *re_first_message = NULL;
static pcre *re_extract_info_first = NULL;
static pcre *re_last_message = NULL;
static pcre *re_extract_message = NULL;


/*
 * Returns the payload if its a first message, else 0 (InvalidTransactionId)
 */
bool
is_first_message(char *buffer, size_t length, char **payload, size_t *payloadLength)
{
	int			ovector[6];
	int			rc;
	if (re_first_message == NULL)
	{
		const char *error = NULL;
		int			erroffset;

		re_first_message = pcre_compile("^{(\"xid\":[0-9]+,\"timestamp\":\"[^\"]+\")", 0, &error, &erroffset, NULL);
		if (error != NULL)
		{
			fatal("init", "Error in regexp");
		}
	}
	rc = pcre_exec(re_first_message, NULL, buffer, length, 0, 0, ovector, 6);
	if (rc < 0)
	{
		return false;
	}
	*payload = &buffer[ovector[2]];
	*payloadLength = ovector[3] - ovector[2];
	return true;
}

bool
is_regular_message(char *buffer, size_t length, char **usefulMessageStart, size_t *usefulMessageLength)
{
	int			ovector[6];
	int			rc;

	if (re_extract_message == NULL)
	{
		const char *error = NULL;
		int			erroffset;

		re_extract_message = pcre_compile("^,?({.*})$", 0, &error, &erroffset, NULL);
		if (error != NULL)
		{
			fatal("init", "Error in regexp");
		}
	}
	rc = pcre_exec(re_extract_message, NULL, buffer, length, 0, 0, ovector, 6);
	if (rc < 0)
	{
		return false;
	}
	*usefulMessageStart = &buffer[ovector[2]];
	*usefulMessageLength = ovector[3] - ovector[2];
	return true;
}


bool
is_last_message(char *buffer, size_t length)
{
	int			ovector[3];

	if (re_last_message == NULL)
	{
		const char *error = NULL;
		int			erroffset;

		re_last_message = pcre_compile("^]}$", 0, &error, &erroffset, NULL);
		if (error != NULL)
		{
			fatal("init", "Error in regexp");
		}
	}
	return pcre_exec(re_last_message, NULL, buffer, length, 0, 0, ovector, 3) > 0;
}

void
extract_first_message(char *buffer, size_t length, LogicalReplicationState *state)
{
	/* Vector for the PCRE results.
	[ 0: capture idx, capture end
	  2:xid start, xid end
	  4:timestamp start, timestamp end
	  6:datetime start, datetime end
	  8:microsecond start, microsecond end, (optional, including dot)
	 10:TZ offset start, TZ offset end
	 12..17: PCRE workspace, to ignore
	] */
	int ovector[18];
	int tz_offset;
	int microseconds;
	struct tm tm;
	if (re_extract_info_first == NULL)
	{
		const char *error = NULL;
		int			erroffset;
		re_extract_info_first = pcre_compile("^\"xid\":(\\d+),\"timestamp\":\"(([\\d-: ]*)(\\.\\d+)?([+-]\\d+))\"", 0, &error, &erroffset, NULL);
		if (error != NULL)
		{
			fatal("init", "Error in regexp");
		}
	}
	int rc = pcre_exec(re_extract_info_first, NULL, buffer, length, 0, 0, ovector, 18);
	if (rc < 0)
	{
		fatal("extract", "Error parsing the first message");
	}

	state->currentXid= strtol(&buffer[ovector[2]], NULL, 10);

	// Extract everything needed to compute the xact_ts
	strptime(&buffer[ovector[6]], "%F %T", &tm);
	asprintf(&(state->timestamp_str), "%.*s", ovector[5] - ovector[4], &buffer[ovector[4]]);
	if (ovector[8] != -1)
	{
		// We capture the . in the string, hence the +1
		microseconds = strtol(&buffer[ovector[8] + 1], NULL, 10);
	}
	else
	{
		// Some timestamps have no microseconds inside
		microseconds = 0;
	}
	tz_offset = strtol(&buffer[ovector[10]], NULL, 10);

	state->xact_ts = (timegm(&tm) - (3600 * tz_offset) - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY)) * 1000000;
	state->xact_ts += microseconds;
}


/*
 *
 * Shamelessly stolen from postgres: src/bin/pg_basebackup/streamutil.c
 *
 * Run IDENTIFY_SYSTEM through a given connection and give back to caller
 * some result information if requested:
 * - System identifier
 * - Current timeline ID
 * - Start LSN position
 * - Database name (NULL in servers prior to 9.4)
 */
bool
RunIdentifySystem(PGconn *conn, char **sysid, uint32_t *starttli,
				  uint64_t *startpos, char **db_name)
{
	PGresult   *res;
	uint32_t	hi,
				lo;

	res = PQexec(conn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("main", "could not send replication command \"%s\": %s",
				 "IDENTIFY_SYSTEM", PQerrorMessage(conn));

	}
	if (PQntuples(res) != 1 || PQnfields(res) < 3)
	{
		log_crit("main",
				 "could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n",
				 PQntuples(res), PQnfields(res), 1, 3);

		PQclear(res);
		return false;
	}

	/* Get system identifier */
	if (sysid != NULL)
		*sysid = strdup(PQgetvalue(res, 0, 0));

	/* Get timeline ID to start streaming from */
	if (starttli != NULL)
		*starttli = atoi(PQgetvalue(res, 0, 1));

	/* Get LSN start position if necessary */
	if (startpos != NULL)
	{
		if (sscanf(PQgetvalue(res, 0, 2), "%X/%X", &hi, &lo) != 2)
		{
			log_crit("main", "could not parse transaction log location \"%s\"\n",
					 PQgetvalue(res, 0, 2));

			PQclear(res);
			return false;
		}
		*startpos = ((uint64_t) hi) << 32 | lo;
	}

	/* Get database name, only available in 9.4 and newer versions */
	if (db_name != NULL)
	{
		*db_name = NULL;
		if (PQserverVersion(conn) >= 90400)
		{
			if (PQnfields(res) < 4)
			{
				fprintf(stderr,
						"could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n",
						PQntuples(res), PQnfields(res), 1, 4);

				PQclear(res);
				return false;
			}
			if (!PQgetisnull(res, 0, 3))
				*db_name = strdup(PQgetvalue(res, 0, 3));
		}
	}

	PQclear(res);
	return true;
}

/* Blatantly stolen from psycopg2, which itself stole it from
 * pgbasebackup/streamutil.c
 */
void
fe_sendint64(int64_t i, char *buf)
{
	uint32_t	n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32_t) (i >> 32);
	n32 = htonl(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32_t) i;
	n32 = htonl(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Converts an int64 from network byte order to native format.
 */
int64_t
fe_recvint64(char *buf)
{
	int64_t		result;
	uint32_t	h32;
	uint32_t	l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}


/*
 * Frontend version of GetCurrentTimestamp(), since we are not linked with
 * backend code. The protocol always uses integer timestamps, regardless of
 * server setting.
 */
int64_t
feGetCurrentTimestamp(void)
{
	int64_t		result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (int64_t) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

/*
 * Blatantly stolen from pg_recvlogical.c
 */
static bool
sendFeedback(LogicalReplicationState * state, int64_t now)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	log_debug("feedback", "Confirming flush up to %X/%X",
			  (uint32_t) (state->lastFlushedLsn >> 32), (uint32_t) state->lastFlushedLsn);
	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(state->lastFlushedLsn, &replybuf[len]);		/* write */
	len += 8;
	fe_sendint64(state->lastFlushedLsn, &replybuf[len]);		/* flush */
	len += 8;
	fe_sendint64(0, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = 0;			/* replyRequested */
	len += 1;

	if (PQputCopyData(state->source_conn, replybuf, len) <= 0 || PQflush(state->source_conn))
	{
		log_crit("feedback", "could not send feedback packet: %s",
				 PQerrorMessage(state->source_conn));
		exit(1);
	}
	state->lastFeedback = feGetCurrentTimestamp();
	if (log_position_timeout < (now - last_logged_position))
	{
		log_info("feedback", "Confirmed feedback up to %X/%X",
				 (uint32_t) (state->lastFlushedLsn >> 32), (uint32_t) state->lastFlushedLsn);
		last_logged_position = now;
	}
	return true;
}

PGconn *
logical_connect(char *source_dsn)
{
	PQconninfoOption *conn_opts;
	PQconninfoOption *conn_opt;
	PGconn	   *conn;
	int			nargs = 0,
				i;
	const char **keys;
	const char **values;
	char	   *errmsg = NULL;

	conn_opts = PQconninfoParse(source_dsn, &errmsg);
	if (errmsg != NULL)
	{
		fatal("main", errmsg);
	}
	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
			nargs++;
	}
	keys = malloc(sizeof(char *) * (nargs + 2));
	values = malloc(sizeof(char *) * (nargs + 2));
	i = 0;
	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
		{
			keys[i] = conn_opt->keyword;
			values[i] = conn_opt->val;
			i++;
		}
	}
	keys[i] = "replication";
	values[i] = "database";
	keys[i + 1] = NULL;
	values[i + 1] = NULL;
	conn = PQconnectdbParams(keys, values, true);
	free(keys);
	free(values);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		log_crit("logical_connect", "Connection to the source db failed: %s", PQerrorMessage(conn));
		exit(1);
	}
	return conn;
}

uint64_t
fetch_last_flushed(PGconn *conn, char *slotName)
{
	uint64_t	flushed_lsn;
	PGresult   *res;
	const char *paramValues[1] = {slotName};

	res = PQexecParams(conn,
					   "SELECT pg_replication_origin_session_setup($1)",
					   1,
					   NULL,
					   paramValues,
					   NULL,
					   NULL,
					   1);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("fetch_last_flushed", "Failed to setup replication origin on target conn: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);

	res = PQexecParams(conn,
			"SELECT pg_replication_origin_progress($1, false) as data_start",
					   1,
					   NULL,
					   paramValues,
					   NULL,
					   NULL,
					   1);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("fetch_last_flushed", "Failed to fetch restart lsn: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	if (PQgetisnull(res, 0, PQfnumber(res, "data_start")))
	{
		flushed_lsn = 0;
	} else
	{
		flushed_lsn = (uint64_t) fe_recvint64(PQgetvalue(res, 0, PQfnumber(res, "data_start")));
	}
	log_info("fetch_last_flushed", "Last flushed value from db is %X/%X",
			 (uint32_t) (flushed_lsn >> 32), (uint32_t) flushed_lsn);
	PQclear(res);
	return flushed_lsn;
}

void
create_table_if_needed(PGconn *conn)
{
	PGresult   *res;

	res = PQexec(conn, "CREATE SCHEMA IF NOT EXISTS replication;");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_crit("create table", "Create schema failed: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	res = PQexec(conn,
				 "CREATE TABLE IF NOT EXISTS replication.raw_messages ("
			 "	insert_timestamp timestamp with time zone default now(),"
				 "	database text,"
				 "	lsn_start pg_lsn,"
				 "	xid xid,"
				 "	payload jsonb);");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_crit("create table", "Create table failed: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	res = PQexec(conn,
				 "ALTER TABLE replication.raw_messages ADD IF NOT EXISTS xid_timestamp timestamptz, "
				 "ADD IF NOT EXISTS source_slotname varchar");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_crit("create table", "Alter table failed: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	res = PQexec(conn,
				"SELECT 1 FROM pg_index WHERE indexrelid::regclass::text = 'replication.raw_messages_order' AND indrelid = 'replication.raw_messages'::regclass");
	if (PQntuples(res) != 1)
	{
		PQclear(res);
		res = PQexec(conn, "CREATE INDEX IF NOT EXISTS raw_messages_order ON replication.raw_messages (insert_timestamp, lsn_start)");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_crit("create table", "Create table failed: %s",
					 PQresultErrorMessage(res));
			exit(1);
		}
	}
	PQclear(res);
}

LogicalReplicationState *
init_replication(char *source_dsn, char *slotname, char *target_dsn, char *filter_tables)
{
	LogicalReplicationState *state = malloc(sizeof(LogicalReplicationState));
	char	   *query;
	char	   *additional_options = NULL;
	PGresult   *res;
	const char *base_start_query = "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X (\"write-in-chunks\" 'true', \"include-types\" 'false', \"include-xids\" 'true', \"include-timestamp\" 'true' %s %s)";

	state->source_conn = logical_connect(source_dsn);
	state->lastFeedback = now = last_logged_position = feGetCurrentTimestamp();
	state->currentXid = 0;		/* Not currently replaying a xact, so
								 * InvalidXID */
	state->slotName = slotname;
	if (!RunIdentifySystem(state->source_conn, NULL, NULL, NULL, &(state->dbname)))
	{
		log_crit("init_replication", "Could not identify system");
		exit(1);
	}

	state->target_conn = PQconnectdb(target_dsn);
	if (PQstatus(state->target_conn) != CONNECTION_OK)
	{
		log_crit("init_replication", "Connection to the target db failed: %s", PQerrorMessage(state->target_conn));
		exit(1);
	}
	log_info("init_replication", "Successfully connected to the target database");
	res = PQexec(state->target_conn, "SET timezone TO 'UTC'");
	if(PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_crit("init_replication", "Could not setup timezone on target database");
		exit(1);
	}
	PQclear(res);
	state->startLsn = fetch_last_flushed(state->target_conn, slotname);
	create_table_if_needed(state->target_conn);
	state->lastFlushedLsn = state->startLsn;
	if (filter_tables != NULL)
	{
		asprintf(&additional_options, ", \"filter-tables\" '%s'", filter_tables);
	} else {
		asprintf(&additional_options, " ");
	}
	/* Try first to start the slot with the formerly-needed, newly-deprecated
	 * option. If it fails, just remove this option and retry. */
	asprintf(&query, base_start_query,
			 slotname, (uint32_t) (state->startLsn >> 32), (uint32_t) state->startLsn, additional_options,
			 ", \"include-unchanged-toast\" 'false'");
	res = PQexec(state->source_conn, query);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		free(query);
		log_info("init_replication", "Init failed, trying without unchanged toast (wal2json >= 2.0)");
		asprintf(&query, base_start_query,
			 slotname, (uint32_t) (state->startLsn >> 32), (uint32_t) state->startLsn, additional_options,
			 "");
		res = PQexec(state->source_conn, query);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			log_crit("init_replication", "could not send replication command \"%s\": %s",
					 query, PQresultErrorMessage(res));
			exit(1);
		}
	}
	free(additional_options);
	log_info("init_replication", "Replication successfully started at LSN %X/%X",
			 (uint32_t) (state->startLsn >> 32), (uint32_t) state->startLsn);
	PQclear(res);
	free(query);

	return state;
}

void
update_replication_origin_standalone(LogicalReplicationState * state)
{
	const char *query = "SELECT pg_replication_origin_advance($1, $2)";
	PGresult *res;
	char lsn[8];
	const char* params[2] = {state->slotName, lsn};
	const int sizes[2] = {strlen(state->slotName), 8};
	const int formats[2] = {1, 1};
	if (PQtransactionStatus(state->target_conn) != PQTRANS_IDLE)
	{
		log_info("update_replication_origin_standalone", "Could not update origin, already in xact");
		return;
	}
	res = PQexec(state->target_conn, "SELECT pg_replication_origin_session_reset()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("update_replication_origin_standalone", "Could not reset replication origin: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	fe_sendint64(state->currentLsn, lsn);
	res = PQexecParams(state->target_conn, query,
					   2, NULL, /* Let postgres figure it out */
					   (const char* const*) params,
					   sizes,
					   formats,
					   0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("update_replication_origin_standalone", "Could not mark xact lsn on target: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	res = PQexecParams(state->target_conn, "SELECT pg_replication_origin_session_setup($1)",
				 1,
				 NULL,
				 params,
				 sizes,
				 formats,
				 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_crit("update_replication_origin_standalone", "Could not reattach session to origin: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);

}

size_t
readMessage(LogicalReplicationState * state, char **buf, struct timeval * timeout, size_t *offset)
{
	int			r = 0;
	fd_set		input_mask;

	now = feGetCurrentTimestamp();
	/* If our keepalive timeout has expired, update the status */
	if (standby_message_timeout > 0 &&
		(standby_message_timeout * 1000) < (now - state->lastFeedback))
	{
		update_replication_origin_standalone(state);
		sendFeedback(state, now);
	}
	if (*buf != NULL)
	{
		PQfreemem(*buf);
		*buf = NULL;
	}
	r = PQgetCopyData(state->source_conn, buf, 1);
	/* No data available: select with a timeout, then return 0 character read */
	if (r == 0)
	{
		FD_ZERO(&input_mask);
		FD_SET(PQsocket(state->source_conn), &input_mask);
		r = select(PQsocket(state->source_conn) + 1, &input_mask, NULL, NULL, timeout);
		if (r == 0 || (r < 0 && errno == EINTR))
		{
			/*
			 * Got a timeout or signal. Continue the loop and either deliver a
			 * status packet to the server or just go back into blocking.
			 */
			return 0;
		}
		else if (r < 0)
		{
			log_crit("readMessage", "select() failed: %s\n",
					 strerror(errno));
			exit(1);
		}
		if (PQconsumeInput(state->source_conn) == 0)
		{
			log_crit("readMessage",
					 "could not receive data from WAL stream: %s",
					 PQerrorMessage(state->source_conn));
			exit(1);
		}
		return 0;
	}
	if (r == -2)
	{
		log_crit("readMessage", "Error while reading COPY data: %s", PQerrorMessage(state->source_conn));
		exit(1);
	}
	/* If we are at the end of the stream, let the caller deal with it */
	if (r == -1)
	{
		return r;
	}
	/* In other cases, extract the message. */
	switch ((*buf)[0])
	{
		case 'k':
			/* Keepalive message */
			{
				int			pos;
				bool		replyRequested;
				uint64_t currentLsn;
				uint64_t sendTime;

				pos = 1;
				pos += 8;		/* walEnd field */
				pos += 8;		/* sendTime field */
				replyRequested = (*buf)[pos];
				/* Extract the current WAL position from the server */
				currentLsn = fe_recvint64(&((*buf)[1]));
				sendTime = fe_recvint64(&((*buf)[2]));
				if(currentLsn != state->currentLsn)
				{
					state->currentLsn = currentLsn;
					state->xact_ts = sendTime;
					state->lastFlushedLsn = currentLsn;
				}

				if (replyRequested)
				{
					update_replication_origin_standalone(state);
					sendFeedback(state, now);
				}
				return 0;
			}
			break;
		case 'w':
			{
				int			hdr_len;
				int			msg_size;

				hdr_len = 1;	/* msgtype 'w' */
				hdr_len += 8;	/* dataStart */
				hdr_len += 8;	/* walEnd */
				hdr_len += 8;	/* sendTime */
				msg_size = r - hdr_len;
				state->currentLsn = fe_recvint64(&((*buf)[1]));
				*offset = hdr_len;
				return msg_size;

			}
		default:
			log_crit("readMessage", "Unknown message type: %c", (*buf)[0]);
			exit(1);
	}
}

enum MessageType
parseMessage(LogicalReplicationState * state, char *message, size_t message_length, char **usefulMessageStart,
			 size_t *usefulMessageLength)
{
	if (is_regular_message(message, message_length, usefulMessageStart, usefulMessageLength))
	{
		return MT_REGULAR;
	}
	if (is_first_message(message, message_length, usefulMessageStart, usefulMessageLength))
	{
		return MT_FIRST;
	}
	if (is_last_message(message, message_length))
	{
		return MT_LAST;
	}
	log_crit("parseMessage", "Could not parse payload!");
	exit(1);
}


void
init_copy(LogicalReplicationState * state)
{
	PGresult   *res = PQexec(state->target_conn, "BEGIN");
	char		header[12] = "PGCOPY\n\377\r\n\0";
	int			flags = 0;		/* Only bit 16 is defined, to include OIDs or
								 * not */

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_crit("send", "Could not start COPY command: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	res = PQexec(state->target_conn,
				 "COPY replication.raw_messages(database, lsn_start, xid, payload, xid_timestamp, source_slotname) FROM STDIN (FORMAT BINARY)");
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		log_crit("send", "Could not start COPY command: %s",
				 PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);
	/* Send header */
	PQputCopyData(state->target_conn, header, 11);
	PQputCopyData(state->target_conn, (void *) &flags, 4);		/* Flags is 4 bytes
																 * worth of 0 */
	PQputCopyData(state->target_conn, (void *) &flags, 4);		/* Extensions field is
																 * also 4 bytes worth of
																 * 0, reuse flags value */
	state->nbMessages = 0;
	log_debug("init_copy", "Started COPY for transaction %d", state->currentXid);
}

void
end_copy(LogicalReplicationState * state)
{
	if (PQputCopyEnd(state->target_conn, NULL) == 1)
	{
		PGresult   *res = PQgetResult(state->target_conn);
		char	   *query;

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_crit("end_copy", "End of copy failed: %s", PQresultErrorMessage(res));
			exit(1);
		}
		PQclear(res);
		asprintf(&query, "SELECT pg_replication_origin_xact_setup('%X/%X', '%s')",
		 (uint32_t) (state->currentLsn >> 32), (uint32_t) state->currentLsn,
		 state->timestamp_str);
		res = PQexec(state->target_conn, query);
		free(query);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_crit("end_copy", "Could not mark xact lsn on target: %s",
					 PQresultErrorMessage(res));
			exit(1);
		}
		PQclear(res);
		res = PQexec(state->target_conn, "COMMIT");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_crit("end_copy", "Failed to commit: %s", PQresultErrorMessage(res));
			exit(1);
		}
		PQclear(res);
		state->lastFlushedLsn = state->currentLsn;
		sendFeedback(state, now);
		log_debug("end_copy", "Commited %d messages for xact %d", state->nbMessages, state->currentXid);
		state->nbMessages = 0;
		state->currentXid = 0;
	}
	else
	{
		log_crit("end_copy", "End of copy failed:%s ", PQerrorMessage(state->target_conn));
		exit(1);
	}
}

void
copy_message(LogicalReplicationState * state, char *payload, size_t payload_length)
{
	short		nbfields = htons(6);
	uint32_t	fieldlen;
	char		buf[8];
	uint32_t	tmpXid;
	char		jsonbversion = 1;
	int         stlen;

	/* Tuple header: 2 fields */
	PQputCopyData(state->target_conn, (void *) &nbfields, 2);
	/* DBName: variable length; */
	stlen = strlen(state->dbname);
	fieldlen = htonl(stlen);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	PQputCopyData(state->target_conn, state->dbname, stlen);
	/* LSN : length 8 */
	fieldlen = htonl(8);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	fe_sendint64(state->currentLsn, buf);
	PQputCopyData(state->target_conn, buf, 8);
	/* XID: length 4 */
	fieldlen = htonl(4);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	tmpXid = htonl(state->currentXid);
	PQputCopyData(state->target_conn, (void *) &tmpXid, 4);
	/* Payload */
	fieldlen = htonl(payload_length + 1);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	PQputCopyData(state->target_conn, &jsonbversion, 1);
	PQputCopyData(state->target_conn, payload, payload_length);
	/* Timestamp: 8bits; */
	fieldlen = htonl(8);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	fe_sendint64(state->xact_ts, buf);
	PQputCopyData(state->target_conn, buf, 8);
	stlen = strlen(state->slotName);
	fieldlen = htonl(stlen);
	PQputCopyData(state->target_conn, (void *) &fieldlen, 4);
	PQputCopyData(state->target_conn, state->slotName, stlen);
	state->nbMessages += 1;
}

void
stream(char *source_dsn, char *target_dsn, char *slotname, char *filter_tables)
{
	LogicalReplicationState *state = init_replication(source_dsn, slotname, target_dsn, filter_tables);
	struct timeval timeout;
	char	   *buf = NULL;

	while (!time_to_abort)
	{
		size_t		offset;
		char	   *payload;
		size_t		payload_length;
		enum MessageType msg_type;
		int			msgLength;

		/* TODO: make the timeout configurable ? */
		timeout.tv_sec = 1;
		timeout.tv_usec = 0;	/* 1 sec */
		msgLength = readMessage(state, &buf, &timeout, &offset);

		/* No data available for now */
		if (msgLength == 0)
			continue;
		/* End of copy stream */
		if (msgLength == -1)
			break;
		msg_type = parseMessage(state, &(buf[offset]), msgLength, &payload, &payload_length);
		switch (msg_type)
		{
			case MT_REGULAR:
				copy_message(state, payload, payload_length);
				continue;
			case MT_FIRST:
				extract_first_message(payload, payload_length, state);
				init_copy(state);
				continue;
			case MT_LAST:
				end_copy(state);
				free(state->timestamp_str);
				continue;
			default:
				log_crit("Main loop", "Unknown message ! %c", msg_type);
				exit(1);
		}
	}
	{
		PGresult   *res;

		res = PQgetResult(state->source_conn);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_crit("main", "unexpected termination of replication stream: %s",
					 PQresultErrorMessage(res));
			exit(1);
		}
		PQclear(res);
		PQfinish(state->source_conn);
		PQfinish(state->target_conn);
		log_info("Main loop", "Successfully ending replication at %X/%X",
				 (uint32_t) (state->lastFlushedLsn >> 32), (uint32_t) state->lastFlushedLsn);
		free(state);
	}
}

