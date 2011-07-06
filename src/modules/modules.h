/* (c) 2011 University of Houston.  All rights reserved. */


#ifndef _MODULES_H
#define _MODULES_H 1

#include "module_info.h"

extern void __shmem_modules_init(void);
extern void __shmem_modules_finalize(void);

extern int __shmem_modules_load(const char *group, char *name, module_info_t *mip);

#endif /* _MODULES_H */
