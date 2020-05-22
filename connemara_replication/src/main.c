/*
 * Copyright (c) 2018 PeopleDoc <dba@people-doc.com >
 *
 */
#include <errno.h>
#include <getopt.h>
#include <stdio.h>

#include "connemara_replication.h"


extern const char *__progname;

#ifndef CONNEMARA_VERSION
#define CONNEMARA_VERSION "dev"
#endif

static const char *PACKAGE_VERSION = CONNEMARA_VERSION;

static void
sigint_handler(int signum)
{
	time_to_abort = true;
}


static void
usage(void)
{
	fprintf(stderr, "Usage: %s [OPTIONS]\n",
			__progname);
	fprintf(stderr, "Version: %s\n", PACKAGE_VERSION);
	fprintf(stderr, "\n");
	fprintf(stderr, " -S, --slot            replication slot to use\n");
	fprintf(stderr, " -d, --debug           be more verbose.\n");
	fprintf(stderr, " -f, --filter-tables   filter theses tables.\n");
	fprintf(stderr, " -h, --help            display help and exit\n");
	fprintf(stderr, " -s, --source          source database dsn\n");
	fprintf(stderr, " -t, --target          target database dsn\n");
	fprintf(stderr, " -v, --version         print version and exit\n");
	fprintf(stderr, "\n");
}

int
main(int argc, char *argv[])
{
	int			debug = 2;
	int			ch;
	char	   *source_dsn = NULL;
	char	   *target_dsn = NULL;
	char	   *slotname = NULL;
	char 	   *filter_tables = NULL;

	static struct option long_options[] = {
		{"debug", no_argument, 0, 'd'},
		{"help", no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{"source", required_argument, 0, 's'},
		{"target", required_argument, 0, 't'},
		{"slot", required_argument, 0, 'S'},
		{"filter-tables", required_argument, 0, 'f'},
		{0}
	};

	while (1)
	{
		int			option_index = 0;

		ch = getopt_long(argc, argv, "hvds:t:S:f:",
						 long_options, &option_index);
		if (ch == -1)
			break;
		switch (ch)
		{
			case 'h':
				usage();
				exit(0);
				break;
			case 'v':
				fprintf(stdout, "%s\n", PACKAGE_VERSION);
				exit(0);
				break;
			case 'd':
				debug++;
				break;
			case 's':
				source_dsn = optarg;
				break;
			case 't':
				target_dsn = optarg;
				break;
			case 'S':
				slotname = optarg;
				break;
			case 'f':
				filter_tables = optarg;
				break;
			default:
				fprintf(stderr, "unknown option `%c'\n", ch);
				usage();
				exit(1);
		}
	}
	if (source_dsn == NULL)
	{
		fatal("main", "The source DSN parameter is mandatory");
	}
	if (target_dsn == NULL)
	{
		fatal("main", "The target DSN parameter is mandatory");
	}
	if (slotname == NULL)
	{
		fatal("main", "The slot parameter is mandatory");
	}
	log_init(debug, __progname);
	signal(SIGINT, sigint_handler);
	/* Internally, we must treat all times as UTC */
	stream(source_dsn, target_dsn, slotname, filter_tables);
	return EXIT_SUCCESS;
}
