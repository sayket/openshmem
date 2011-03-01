#ifndef _UTILS_H
#define _UTILS_H 1

#ifdef DEBUG

#include <stdio.h>
#include <stdlib.h>

#include "state.h"

/*
 * if we haven't initialized through start_pes() then try to do
 * something constructive.  Obviously can't use __shmem_trace()
 * because nothing has been initialized.
 *
 */

#define INIT_CHECK()							\
  do {									\
    if (__state.pe_status != PE_RUNNING) {				\
      fprintf(stderr,							\
	      "Error: OpenSHMEM library has not been initialized\n"	\
	      );							\
      exit(1);								\
      /* NOT REACHED */							\
    }									\
  } while (0)

#else /* ! DEBUG */

#define INIT_CHECK()

#endif /* DEBUG */

#define TABLE_SIZE(T) ( sizeof(T) / sizeof((T)[0]) )

#endif /* _UTILS_H */