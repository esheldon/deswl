/*
   Basic library to work with a Multi Epoch Data Structure

   Only use simple arrays for images to keep dependencies low.  The user can
   then wrap this to use their favorite image library.
*/
#ifndef _MEDS_INCLUDE_HEADER_GUARD
#define _MEDS_INCLUDE_HEADER_GUARD

#include <fitsio.h>

#define MEDS_NCOLUMNS 11
struct meds_obj {
    long     id;
    long     ncutout;
    long     box_size;

    long    *file_id;
    long    *start_row;
    double  *orig_row;
    double  *orig_col;
    long    *orig_start_row;
    long    *orig_start_col;
    double  *cutout_row;
    double  *cutout_col;
};

struct meds_cat {
    long size;
    struct meds_obj *data;
};

struct meds_image_info {
    long flen;
    char* filename;
};
struct meds_info_cat {
    long size;
    struct meds_image_info *data;
};

struct meds {
    char *filename;
    fitsfile *fits;
    struct meds_cat *cat;
    struct meds_info_cat *image_info;
};

struct meds_cutout {
    long size;        // ncutouts*nrows*ncols
    long ncutouts;
    long nrows;       // per cutout
    long ncols;       // per cutout
    double **rows;
};

#define CUTOUT_SIZE(im) ((im)->size)
#define CUTOUT_NCUTOUT(im) ((im)->ncutouts)
#define CUTOUT_NROWS(im) ((im)->nrows)
#define CUTOUT_NCOLS(im) ((im)->ncols)

#define CUTOUT_GET(im, row, col)                  \
    ( *((im)->rows[(row)] + (col)) )
#define MOSAIC_GET(im, ncutout, row, col)                  \
    ( *((im)->rows[(ncutout)*(im)->nrows + (row)] + (col)) )


// open a meds structure
struct meds *meds_open(const char *filename);
// free the structure
struct meds *meds_free(struct meds *self);

// get the full catalog
const struct meds_cat *meds_get_cat(const struct meds *self);
// get an entry in the catalog
const struct meds_obj *meds_get_obj(const struct meds *self, long iobj);

// read a single cutout
double *meds_get_cutout(long iobj, long icutout, long *nrow, long *ncol);
// read a cutout mosaic
double *meds_get_mosaic(long iobj, long *ncutout, long *nrow, long *ncol);

// get info for the source image of the indicated cutout
const struct meds_image_info *meds_get_source_info(const struct meds *self,
                                                   long iobj,
                                                   long icutout);

// get the filename for the source image of the indicated cutout
long meds_get_source_file_id(const struct meds *self,
                             long iobj,
                             long icutout);

// get the filename for the source image of the indicated cutout
const char *meds_get_source_filename(const struct meds *self,
                                     long iobj,
                                     long icutout);


// print tools
void meds_print(const struct meds *self, FILE* stream);
void meds_obj_print(const struct meds_obj *obj, FILE* stream);
void meds_image_info_print(const struct meds_image_info *self, FILE* stream);

#endif
