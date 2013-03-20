#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "meds.h"

void test_cutout(struct meds *meds)
{

    long nobj=meds_get_size(meds);


    int found=0;
    int istart=nobj/4;
    for (long iobj=istart; iobj<nobj; iobj++) {
        long ncutout=meds_get_ncutout(meds, iobj);

        if (ncutout > 1) {
            printf("iobj: %ld\n", iobj);
            printf("    ncutout: %ld\n", ncutout);

            long icutout=1;

            struct meds_cutout *cut     = meds_get_cutout(meds, iobj, icutout);
            struct meds_cutout *wcut    = meds_get_weight_cutout(meds, iobj, icutout);
            struct meds_cutout *skycut  = meds_get_sky_cutout(meds, iobj, icutout);
            struct meds_cutout *mosaic  = meds_get_mosaic(meds, iobj);
            struct meds_cutout *wmosaic = meds_get_weight_mosaic(meds, iobj);
            struct meds_cutout *skymosaic = meds_get_sky_mosaic(meds, iobj);

            assert(cut);
            assert(wcut);
            assert(mosaic);
            assert(wmosaic);

            printf("    cutout %ld nrow: %ld ncol: %ld\n", 
                    icutout, CUTOUT_NROW(cut), CUTOUT_NCOL(cut));
            printf("    mosaic %ld nrow: %ld ncol: %ld\n", 
                    icutout, MOSAIC_NROW(mosaic), MOSAIC_NCOL(mosaic));

            long row=2+CUTOUT_NROW(cut)/2, col=3+CUTOUT_NCOL(cut)/2;

            printf("    pixel [%ld,%ld]:\n", row, col);
            printf("        from single cutout: %g\n", 
                    CUTOUT_GET(cut, row, col));
            printf("        from mosaic:        %g\n", 
                    MOSAIC_GET(mosaic, icutout, row, col));


            printf("        from single weight cutout: %g\n", 
                    CUTOUT_GET(wcut, row, col));
            printf("        from weight mosaic:        %g\n", 
                    MOSAIC_GET(wmosaic, icutout, row, col));

            printf("        from single sky cutout: %g\n", 
                    CUTOUT_GET(skycut, row, col));
            printf("        from sky mosaic:        %g\n", 
                    MOSAIC_GET(skymosaic, icutout, row, col));

            double drow=0,dcol=0;
            meds_get_cutout_cen(meds, iobj, icutout, &drow, &dcol);
            long irow=(int)drow;
            long icol=(int)dcol;

            printf("    cen pixel [%lf,%lf]:\n", drow, dcol);
            printf("        from single cutout: %g\n", 
                    CUTOUT_GET(cut, irow, icol));
            printf("        from mosaic:        %g\n", 
                    MOSAIC_GET(mosaic, icutout, irow, icol));
            printf("        sky:                %g\n", 
                    MOSAIC_GET(skymosaic, icutout, irow, icol));
            printf("        sky subtracted:     %g\n", 
                    MOSAIC_GET(mosaic, icutout, irow, icol)
                    -
                    MOSAIC_GET(skymosaic, icutout, irow, icol));



            assert(ncutout == MOSAIC_NCUTOUT(mosaic));
            assert(ncutout == MOSAIC_NCUTOUT(wmosaic));

            assert(MOSAIC_SIZE(mosaic) == ncutout*CUTOUT_SIZE(cut));
            assert(MOSAIC_SIZE(wmosaic) == ncutout*CUTOUT_SIZE(cut));
            assert(MOSAIC_NROW(mosaic) == ncutout*CUTOUT_NROW(cut));
            assert(MOSAIC_NROW(wmosaic) == ncutout*CUTOUT_NROW(cut));

            assert(MOSAIC_GET(mosaic,icutout,row,col)
                        ==CUTOUT_GET(cut, row, col));
            assert(MOSAIC_GET(wmosaic,icutout,row,col)
                        ==CUTOUT_GET(wcut, row, col));

            cut=meds_cutout_free(cut);
            wcut=meds_cutout_free(wcut);
            skycut=meds_cutout_free(skycut);
            mosaic=meds_cutout_free(mosaic);
            wmosaic=meds_cutout_free(wmosaic);
            skymosaic=meds_cutout_free(skymosaic);

            assert(NULL==cut);

            found=1;
            break;
        }
    }

    if (!found) {
        printf("didn't find any objects with > 1 cutouts\n");
    }
}



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

    long nobj=meds_get_size(meds);
    meds_obj_print(&meds->cat->data[0], stdout);
    meds_obj_print(&meds->cat->data[nobj-1], stdout);

    long nimage=meds->image_info->size;
    meds_image_info_print(&meds->image_info->data[0], stdout);
    meds_image_info_print(&meds->image_info->data[nimage-1], stdout);

    test_cutout(meds);

    meds=meds_free(meds);
}
