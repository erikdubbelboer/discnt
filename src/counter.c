
#include "discnt.h"
#include "counter.h"


void dictCounterDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */

    /* TODO: Discnt: Do we need to free counter.replicas here? */
}
