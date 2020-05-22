#ifndef __CONNEMARA_REPLICATION_H__
#define __CONNEMARA_REPLICATION_H__

/*
 * Copyright (c) 2018 PeopleDoc <dba@people-doc.com >
 *
 */

#include <errno.h>
#include <libpq-fe.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "log.h"

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE	2440588		/* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define SECS_PER_DAY	86400
#define USECS_PER_SEC	1000000LL

extern volatile sig_atomic_t time_to_abort;

typedef struct LogicalReplicationState
{
	PGconn	   *source_conn;
	PGconn	   *target_conn;
	PGresult   *target_result;
	uint64_t	startLsn;
	uint64_t	currentLsn;
	uint64_t	walEnd;
	uint64_t	lastFlushedLsn;
	char	   *dbname;
	uint32_t	currentXid;
	int64_t		lastFeedback;
	int			nbMessages;
	int64_t	xact_ts;
	char        *timestamp_str;
	char 		*slotName;
}	LogicalReplicationState;

enum MessageType
{
	MT_FIRST,
	MT_LAST,
	MT_REGULAR,
};


/*
 * Returns the xid if its a first message, else 0 (InvalidTransactionId)
 */
bool
is_first_message(char *buffer, size_t length, char **payload, size_t *payloadLength);

bool
is_regular_message(char *buffer, size_t length, char **usefulMessageStart, size_t *usefulMessageLength);

bool
is_last_message(char *buffer, size_t length);

void
extract_first_message(char *buffer, size_t length, LogicalReplicationState *state);

PGconn *
logical_connect(char *source_dsn);

uint64_t
fetch_last_flushed(PGconn *conn, char *slotName);

void
create_table_if_needed(PGconn *conn);

LogicalReplicationState *
init_replication(char *source_dsn, char *slotname, char *target_dsn, char *filter_tables);

size_t
readMessage(LogicalReplicationState * state, char **buf, struct timeval * timeout, size_t *offset);

enum MessageType
parseMessage(LogicalReplicationState * state, char *message, size_t message_length, char **usefulMessageStart,
			 size_t *usefulMessageLength);

void
init_copy(LogicalReplicationState * state);

void
end_copy(LogicalReplicationState * state);

void
copy_message(LogicalReplicationState * state, char *payload, size_t payload_length);

void
stream(char *source_dsn, char *target_dsn, char *slotname, char *filter_tables);


#endif
