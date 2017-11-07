/*
 *
 * Copyright (c) 2016
 *   Stony Brook University
 * Copyright (c) 2015 - 2016
 *   Los Alamos National Security, LLC.
 * Copyright (c) 2011 - 2016
 *   University of Houston System and UT-Battelle, LLC.
 * Copyright (c) 2009 - 2016
 *   Silicon Graphics International Corp.  SHMEM is copyrighted
 *   by Silicon Graphics International Corp. (SGI) The OpenSHMEM API
 *   (shmem) is released by Open Source Software Solutions, Inc., under an
 *   agreement with Silicon Graphics International Corp. (SGI).
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * o Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimers.
 *
 * o Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *
 * o Neither the name of the University of Houston System,
 *   UT-Battelle, LLC. nor the names of its contributors may be used to
 *   endorse or promote products derived from this software without specific
 *   prior written permission.
 *
 * o Neither the name of Los Alamos National Security, LLC, Los Alamos
 *   National Laboratory, LANL, the U.S. Government, nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
 * This file contains only the experimental eureka feaures.
 */
#include <stdio.h>

 
#ifdef HAVE_FEATURE_EXPERIMENTAL
#include "shmemx.h"
#include "eureka.h"
#include "comms/comms.h"


/*
 * Default clean up handler for eureka function 
 */
static void _eureka_default_clean_up_handler( void *args)
{
	//Does nothing
}


static void *_shmem_eureka_init (void *args)
{
	int ret, oldtype;
	const int npes = GET_STATE (numpes);
	const int mype = GET_STATE (mype);
	
	shmemi_trace (SHMEM_LOG_EUREKA, "2: Address of pthread_t = %p ", &eureka_obj.eureka_thrd);
	/* 
	 *Set the appropitate eureka_thread clean up handler
	 *Checking for NULL, because if the handler is NULL, it will crash. 
	 *This allows user to pass NULL without having any side effect ( So nice of me :v :v)
	 */

	pthread_cleanup_push((eureka_obj.clean_up_handler), (eureka_obj.clean_up_handler_args));	

	/* Enable aynchronus cancllation of the thread */	
	ret = pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &oldtype);
	
	if(ret != 0)
	{
		shmemi_trace (SHMEM_LOG_EUREKA, "Error: setting the eureka thread property");
		goto out;
	}
	
	/* Now call the eureka_func */
	eureka_obj.eureka_func(eureka_obj.eureka_func_args);
	
	
	
out:
	shmemi_trace (SHMEM_LOG_EUREKA, "_shmem_eureka_init() done");
	pthread_cleanup_pop(0);
	
	return NULL;
}

/*
 *@summary Initializes the an eureka_event
 *@param eureka_func a function pointer pointing to the erueka code region
 *@param eureka_func_args arguments for the function pointer 
 *@param eureka_clean_up_handler a clean_up function of the eureka_func
 *TODO: have to make sure this function returns same value in all PEs(collective)
 */
void shmem_eureka_init (void (*eureka_func) (void*), void *eureka_func_args, void (*eureka_clean_up_handler) (void*), void *clean_up_handler_args)
{

	int ret, i;
	void *res;
	
	const int npes = GET_STATE (numpes);
	const int mype = GET_STATE (mype);
	
	/* 
	 *Fill up the eureka_obj for the eureka_event
	 *This eureka_container_t object will be used throughout the 
	 *Eureka event
	 */	
	eureka_obj.eureka_func = eureka_func;	
	eureka_obj.eureka_func_args = eureka_func_args;

	if(eureka_clean_up_handler == NULL)
	{
		eureka_obj.clean_up_handler = _eureka_default_clean_up_handler;
		eureka_obj.clean_up_handler_args = NULL;
	}
	else
	{
		eureka_obj.clean_up_handler = eureka_clean_up_handler;	
		eureka_obj.clean_up_handler_args = clean_up_handler_args;
	}
	
	
	/*implicit barrier before _shmem_eureka_init*/
	shmem_barrier_all ();
	
	/* Create the pthread and assign the eureka_func work to this thread */
	
	shmemi_trace (SHMEM_LOG_EUREKA, "1: Address of pthread_t = %p ", &eureka_obj.eureka_thrd);
	
	ret = pthread_create(&eureka_obj.eureka_thrd, NULL, &_shmem_eureka_init, NULL);
    if (ret != 0)
	{
		shmemi_trace (SHMEM_LOG_EUREKA, "Error: creating eureka thread");
	}
	
	shmemi_trace (SHMEM_LOG_EUREKA, "5: Address of pthread_t = %p ", &eureka_obj.eureka_thrd);
	
	ret = pthread_join(eureka_obj.eureka_thrd, &res);
    if (ret != 0)
	{
		shmemi_trace (SHMEM_LOG_EUREKA, "Error: joining eureka thread");
	}

	if (res == PTHREAD_CANCELED)
    {
		shmemi_trace (SHMEM_LOG_EUREKA, "Eureka Thread was cancelled");
	}
    else
    {
		shmemi_trace (SHMEM_LOG_EUREKA, "Eureka Thread terminated normally");
	}

	/*
	 *implicit barrier before
	 *We may not need this (as we are joining the threads)
	 */
	shmem_barrier_all ();
	
	//return ret;	
}

/*
 *@summary Triggers a eureka event, starts the termination process out of the eureka_region
 */
void shmem_eureka (void)
{
	const int mype = GET_STATE (mype);

	shmemi_trace (SHMEM_LOG_EUREKA,
                      "Begin shmem_eureka_trigger()");
	shmemi_trace (SHMEM_LOG_EUREKA, "3: Address of pthread_t = %p ", &eureka_obj.eureka_thrd);
	shmemi_comms_eureka_tirgger_request(mype);
	shmemi_trace (SHMEM_LOG_EUREKA,
                      "End shmem_eureka_trigger()");

}

#endif /* HAVE_FEATURE_EXPERIMENTAL */
 
