#include <stdio.h>
#include <stdlib.h>

#include "meds.h"

int main(int argc, char **argv)
{
    if (argc < 2) {
        printf("usage: test meds_file\n");
        exit(1);
    }

    const char *meds_file=argv[1];

    printf("opening meds file: %s\n", meds_file);
    struct meds *meds=meds_open(meds_file);
    if (!meds) {
        fprintf(stderr,"error reading meds, exiting\n");
        exit(1);
    }

    meds_print(meds, stdout);

    long nobj=meds->cat->size;
    meds_obj_print(&meds->cat->data[0], stdout);
    meds_obj_print(&meds->cat->data[nobj-1], stdout);

    long nimage=meds->image_info->size;
    meds_image_info_print(&meds->image_info->data[0], stdout);
    meds_image_info_print(&meds->image_info->data[nimage-1], stdout);

    meds=meds_free(meds);
}
