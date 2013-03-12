/*
   Basic library to work with a Multi Epoch Data Structure

   If you have your own image library, you can use the meds_get_cutoutp and
   meds_get_mosaicp to get raw pointers that can be wrapped by your library.

   You can also use image structures returned by meds_get_cutout and
   meds_get_mosaic

   A few examples
   --------------

   #include <meds.h>

   struct meds *meds=meds_open(filename);
   meds_print(meds, stdout);

   long nobj=meds_get_size(meds);

   long iobj=35;
   if (meds_get_ncutout(meds, iobj) > 0) {

       //
       // get the 4th cutout for this object (at index=3)
       //

       long icutout=3;

       // using the cutout structure
       struct meds_cutout *cutout=meds_get_cutout(meds, iobj, icutout);

       printf("nrow: %ld ncol: %ld\n", CUTOUT_NROW(cutout), CUTOUT_NCOL(cutout));
       long row=5, col=8;
       printf("pixel [%ld,%ld]: %g\n", row, col, CUTOUT_GET(cutout, row, col));

       // using the pointer interface; good if you have your own image library
       long nrow=0, ncol=0;
       double *pix=meds_get_cutoutp(meds, iobj, icutout, &nrow, &ncol);


       // what file did the cutout come from?
       const char *name=meds_get_source_filename(meds, iobj, icutout);


       //
       // get a mosaic of all cutouts for this object
       //

       // using the cutout structure
       struct meds_cutout *mosaic=meds_get_mosaic(meds, iobj);

       printf("ncutout: %ld\n", MOSAIC_NCUTOUT(mosaic));
       printf("per cutout, nrow: %ld ncol: %ld\n", CUTOUT_NROW(mosaic), CUTOUT_NCOL(mosaic));

       // This should agree with printout for the single cutout above
       printf("value at [%ld,%ld]\n", row, col, MOSAIC_GET(mosaic, icutout, row, col));


       // use the pointer interface; good if you have your own image library
       long ncutout=0, nrow=0, ncol=0;
       double *mpix=meds_get_mosaicp(meds, iobj, &ncutout, &nrow, &ncol);


       free(pix);pix=NULL;
       free(mpix);mpix=NULL;

       // free the cutout structures.  They are set to NULL.
       cutout=meds_cutout_free(cutout);
       mosaic=meds_cutout_free(mosaic);

   }
   
   // the meds_obj structure contains additional information such as
   // where the cutouts were located in the original source images.
   // see the struct definition for details
   // get a meds_obj structure for an object using
   // 
   const struct meds_obj *obj=meds_get_obj(meds, iobj);
   meds_obj_print(obj, stdout);

*/
#ifndef _MEDS_INCLUDE_HEADER_GUARD
#define _MEDS_INCLUDE_HEADER_GUARD

#include <fitsio.h>

#define MEDS_NCOLUMNS 11

struct meds_obj {
    long     id;             // id column from coadd catalog
    long     ncutout;        // number of cutouts for this object, including coadd
    long     box_size;       // cutout size is box_sizeXbox_size

    long    *file_id;        // index into the image_info structure for this cutout
    long    *start_row;      // zero-offset row in the big mosaic image of all cutouts
                             // for all objects in the MEDS file
    double  *orig_row;       // zero-offset center row in original image
    double  *orig_col;       // zero-offset center col in original image
    long    *orig_start_row; // zero-offset start row in original image
    long    *orig_start_col; // zero-offset start col in original image
    double  *cutout_row;     // zero-offset center row in cutout image
    double  *cutout_col;     // zero-offset center col in cutout image
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

// open a meds structure
struct meds *meds_open(const char *filename);

// free the structure.  Returns NULL.  Use like this:
//    m=meds_free(m);
struct meds *meds_free(struct meds *self);

// number of entries in the catalog
long meds_get_size(const struct meds *self);

// the meds cutout filename
const char *meds_get_filename(const struct meds *self);

// get an entry in the catalog
// if index does not exist, NULL is returned
const struct meds_obj *meds_get_obj(const struct meds *self, long iobj);

// return obj->ncutout for the indicated object
// if index does not exist, zero is returned
long meds_get_ncutout(const struct meds *self, long iobj);

// you can also work with the catalog
const struct meds_cat *meds_get_cat(const struct meds *self);

// read a single cutout as a simple pointer
// 
// returns the pixel array.  The number of rows and columns are stored
// in nrow,ncol entered by the user.
double *meds_get_cutoutp(const struct meds *self,
                         long iobj,
                         long icutout,
                         long *nrow,
                         long *ncol);

// read a cutout mosaic as a simple pointer
//
// returns the pixel array.  The number of cutouts, rows and columns are stored
// in nrow,ncol entered by the user.
double *meds_get_mosaicp(const struct meds *self,
                         long iobj,
                         long *ncutout,
                         long *nrow,
                         long *ncol);

// optionally get the cutouts as a simple structure


// get info for the source image of the indicated cutout
const struct meds_image_info *meds_get_source_info(const struct meds *self,
                                                   long iobj,
                                                   long icutout);

// get the file_id for the source image of the indicated cutout.  This
// id points into the image_info structure.  e.g.
//    long id=meds_get_source_file_id(meds, iobj, icutout);
//    const struct meds_image_info *info = 
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



//
// simple image class for cutouts/mosaics
//
struct meds_cutout {
    long ncutout;

    long mosaic_size;        // ncutout*nrow*ncol
    long mosaic_nrow;        // ncutout*nrow
    long mosaic_ncol;        // ncol

    long cutout_size;       // nrow*ncol
    long cutout_nrow;       // per cutout
    long cutout_ncol;       // per cutout
    double **rows;
};

#define CUTOUT_SIZE(im) ((im)->cutout_size)
#define CUTOUT_NROW(im) ((im)->cutout_nrow)
#define CUTOUT_NCOL(im) ((im)->cutout_ncol)

#define CUTOUT_GET(im, row, col)                               \
    ( *((im)->rows[(row)] + (col)) )

#define MOSAIC_NCUTOUT(im) ((im)->ncutout)
#define MOSAIC_SIZE(im) ((im)->mosaic_size)
#define MOSAIC_NROW(im) ((im)->mosaic_nrow)
#define MOSAIC_NCOL(im) ((im)->mosaic_ncol)

#define MOSAIC_GET(im, cutout, row, col)                       \
    ( *((im)->rows[(cutout)*(im)->cutout_nrow + (row)] + (col)) )

// read a single cutout.  Use CUTOUT_GET or MOSAIC_GET(0, row,col) to
// access pixels
struct meds_cutout *meds_get_cutout(const struct meds *self,
                                    long iobj,
                                    long icutout);
// read a cutout mosaic.  Use MOSAIC_GET to access pixels
struct meds_cutout *meds_get_mosaic(const struct meds *self,
                                    long iobj);

// returns NULL, use like this
//   cutout=meds_cutout_free(cutout);
struct meds_cutout *meds_cutout_free(struct meds_cutout *self);

#endif
