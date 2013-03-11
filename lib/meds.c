#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fitsio.h>

#include "meds.h"

//
// some static methods and variables
//

static char *MEDS_COLNAMES[]={
    "id", "ncutout", "box_size",
    "file_id", "start_row", 
    "orig_row", "orig_col","orig_start_row","orig_start_col",
    "cutout_row","cutout_col"};

static long *alloc_longs(long n) {
    long *ptr=malloc(n*sizeof(long));
    if (!ptr) {
        fprintf(stderr,"could not %ld longs\n", n);
        exit(1);
    }
    for (long i=0; i<n; i++) {
        ptr[i]=-9999;
    }
    return ptr;
}
static double *alloc_doubles(long n) {
    double *ptr=malloc(n*sizeof(double));
    if (!ptr) {
        fprintf(stderr,"could not %ld doubles\n", n);
        exit(1);
    }
    for (long i=0; i<n; i++) {
        ptr[i]=-9999;
    }
    return ptr;
}

static void print_doubles(const double* vals, long n, const char *name, FILE* stream)
{
    fprintf(stream,"%-14s : ", name);
    for (long i=0; i<n; i++) {
        fprintf(stream," %lf", vals[i]);
    }
    fprintf(stream,"\n");
}
static void print_longs(const long *vals, long n, const char *name, FILE* stream)
{
    fprintf(stream,"%-14s : ", name);
    for (long i=0; i<n; i++) {
        fprintf(stream," %ld", vals[i]);
    }
    fprintf(stream,"\n");
}

static fitsfile *fits_open(const char *filename)
{
    int status=0;
    fitsfile *fits=NULL;
    if (fits_open_file(&fits, filename, READONLY, &status)) {
        fits_report_error(stderr,status);
        return NULL;
    }
    return fits;
}
static fitsfile *fits_close(fitsfile *fits)
{
    int status=0;
    if (fits_close_file(fits, &status)) {
        fits_report_error(stderr,status);
    }
    return NULL;
}

static long get_nrows(fitsfile *fits)
{
    int status=0;
    long nrows=0;
    if (fits_get_num_rows(fits, &nrows, &status)) {
        fits_report_error(stderr,status);
        return 0;
    }
    return nrows;
}

static int get_colnum(fitsfile *fits, const char *colname)
{
    int status=0;
    int colnum=0;
    if (fits_get_colnum(fits, 0, (char*)colname, &colnum, &status)) {
        fits_report_error(stderr,status);
        return -9999;
    }
    return colnum;
}
static int get_colnums(fitsfile *fits, int *colnums)
{
    for (int i=0; i<MEDS_NCOLUMNS; i++) {
        int colnum=get_colnum(fits, MEDS_COLNAMES[i]);
        if (colnum < 0) {
            return 0;
        }
        colnums[i] = colnum;
    }
    return 1;
}

static int fits_read_doubles(fitsfile *fits, 
                             int colnum,
                             LONGLONG row,
                             LONGLONG nelem,
                             double *data)
{
    int status=0;
    int nullval=0;
    LONGLONG firstelem=1;
    if (fits_read_col_dbl(fits, colnum, row, firstelem, nelem,
                          nullval, data, NULL, &status)) {
        fits_report_error(stderr,status);
        return 0;
    }
    return 1;
}
static int fits_read_longs(fitsfile *fits, 
                           int colnum,
                           LONGLONG row,
                           LONGLONG nelem,
                           long *data)
{
    int status=0;
    int nullval=0;
    LONGLONG firstelem=1;
    if (fits_read_col_lng(fits, colnum, row, firstelem, nelem,
                          nullval, data, NULL, &status)) {
        fits_report_error(stderr,status);
        return 0;
    }
    return 1;
}


// size of a fixed-length array column (per row)
static long get_array_col_size(fitsfile *fits, const char *colname)
{
    int status=0;
    int maxdim=2;
    int naxis = {0};
    long naxes[2];

    int colnum=0;
    if (fits_get_colnum(fits, 0, (char*)colname, &colnum, &status)) {
        fits_report_error(stderr,status);
        return 0;
    }
    if (fits_read_tdim(fits, colnum, maxdim, &naxis,
                       naxes, &status)) {
        fits_report_error(stderr,status);
        return 0;
    }
    return naxes[0];
}



//
// struct meds_obj
//

static void alloc_fields(struct meds_obj *self, long ncutout)
{
    self->file_id        = alloc_longs(ncutout);
    self->start_row      = alloc_longs(ncutout);
    self->orig_row       = alloc_doubles(ncutout);
    self->orig_col       = alloc_doubles(ncutout);

    self->orig_start_row = alloc_longs(ncutout);
    self->orig_start_col = alloc_longs(ncutout);

    self->cutout_row     = alloc_doubles(ncutout);
    self->cutout_col     = alloc_doubles(ncutout);
}
static void free_fields(struct meds_obj *self)
{
    if (self) {
        free(self->file_id);
        free(self->start_row);
        free(self->orig_row);
        free(self->orig_col);

        free(self->orig_start_row);
        free(self->orig_start_col);

        free(self->cutout_row);
        free(self->cutout_col);
    }
}

void meds_obj_print(const struct meds_obj *self, FILE* stream)
{
    fprintf(stream,"%-14s : %ld\n", "id", self->id);
    fprintf(stream,"%-14s : %ld\n", "ncutout", self->ncutout);
    fprintf(stream,"%-14s : %ld\n", "box_size",   self->box_size);
    print_longs(self->file_id, self->ncutout, "file_id", stream);
    print_longs(self->start_row, self->ncutout, "start_row", stream);
    print_doubles(self->orig_row, self->ncutout, "orig_row", stream);
    print_doubles(self->orig_col, self->ncutout, "orig_col", stream);
    print_longs(self->orig_start_row, self->ncutout, "orig_start_row", stream);
    print_longs(self->orig_start_col, self->ncutout, "orig_start_col", stream);
    print_doubles(self->cutout_row, self->ncutout, "cutout_row", stream);
    print_doubles(self->cutout_col, self->ncutout, "cutout_col", stream);
}

//
// struct meds_cat
//

static struct meds_cat *meds_cat_new(long size, long ncutout_max)
{
    struct meds_cat *self=calloc(1, sizeof(struct meds_cat));
    if (!self) {
        fprintf(stderr,"could not allocate struct meds_cat\n");
        exit(1);
    }

    self->size=size;
    self->data=calloc(size, sizeof(struct meds_obj));
    if (!self->data) {
        fprintf(stderr,"could not allocate %ld meds_obj structures\n", size);
        exit(1);
    }

    struct meds_obj *obj=self->data;
    for (long i=0; i<size; i++) {
        alloc_fields(obj, ncutout_max);
        obj->ncutout=ncutout_max;
        obj++;
    }

    return self;
}

static struct meds_cat *meds_cat_free(struct meds_cat *self)
{
    if (self) {
        struct meds_obj *obj=self->data;
        for (long i=0; i<self->size; i++) {
            free_fields(obj);
            obj++;
        }
        free(self->data);
        free(self);
    }
    return NULL;
}

static int load_table(struct meds_cat *self, fitsfile *fits)
{
    int colnums[MEDS_NCOLUMNS];
    if (!get_colnums(fits, colnums)) {
        return 0;
    }

    struct meds_obj *obj=self->data;
    for (long i=0; i < self->size; i++) {
        long row = i+1;
        if (!fits_read_longs(fits, colnums[0], row, 1, &obj->id))
            return 0;
        if (!fits_read_longs(fits, colnums[1], row, 1, &obj->ncutout))
            return 0;
        if (!fits_read_longs(fits, colnums[2], row, 1, &obj->box_size))
            return 0;
        if (!fits_read_longs(fits, colnums[3], row, obj->ncutout, obj->file_id))
            return 0;
        if (!fits_read_longs(fits, colnums[4], row, obj->ncutout, obj->start_row))
            return 0;
        if (!fits_read_doubles(fits, colnums[5], row, obj->ncutout, obj->orig_row))
            return 0;
        if (!fits_read_doubles(fits, colnums[6], row, obj->ncutout, obj->orig_col))
            return 0;
        if (!fits_read_longs(fits, colnums[7], row, obj->ncutout, obj->orig_start_row))
            return 0;
        if (!fits_read_longs(fits, colnums[8], row, obj->ncutout, obj->orig_start_col))
            return 0;
        if (!fits_read_doubles(fits, colnums[9], row, obj->ncutout, obj->cutout_row))
            return 0;
        if (!fits_read_doubles(fits, colnums[10], row, obj->ncutout, obj->cutout_col))
            return 0;

        obj++;
    }

    return 1;
}

static struct meds_cat *read_object_table(fitsfile *fits)
{
    int status=0;
    if (fits_movnam_hdu(fits, BINARY_TBL, "object_data", 0, &status)) {
        fits_report_error(stderr,status);
        return NULL;
    }

    long nrows=get_nrows(fits);
    if (!nrows) {
        return NULL;
    }
    //printf("found %ld objects in main table\n", nrows);

    long ncutout_max = get_array_col_size(fits, "file_id");
    if (!nrows) {
        return NULL;
    }
    //printf("ncutout max: %ld\n", ncutout_max);

    struct meds_cat *self=meds_cat_new(nrows, ncutout_max);

    if (!load_table(self, fits)) {
        self=meds_cat_free(self);
        return NULL;
    }
    return self;
}


//
// struct meds_image_info
//

void meds_image_info_print(const struct meds_image_info *self, FILE* stream)
{
    fprintf(stream,"filename: %s\n", self->filename);
}


//
// struct meds_info_cat
//

static struct meds_info_cat *meds_info_cat_new(long size, long namelen_max)
{
    struct meds_info_cat *self=calloc(1, sizeof(struct meds_info_cat));
    if (!self) {
        fprintf(stderr,"could not allocate struct meds_info_cat\n");
        exit(1);
    }

    self->size=size;
    self->data=calloc(size, sizeof(struct meds_image_info));
    if (!self->data) {
        fprintf(stderr,"could not allocate %ld meds_image_info structures\n", size);
        exit(1);
    }

    struct meds_image_info *info=self->data;
    for (long i=0; i<size; i++) {
        info->filename = calloc(namelen_max, sizeof(char));
        if (!info->filename) {
            fprintf(stderr,"failed to allocate filename of size %ld\n", namelen_max);
            exit(1);
        }
        info->flen=namelen_max;
        info++;
    }

    return self;
}

static struct meds_info_cat *meds_info_cat_free(struct meds_info_cat *self)
{
    if (self) {
        struct meds_image_info *info=self->data;
        for (long i=0; i<self->size; i++) {
            free(info->filename);
            info++;
        }
        free(self->data);
        free(self);
    }
    return NULL;
}


static int load_image_info(struct meds_info_cat *self, fitsfile *fits)
{
    int status=0;
    int colnum=1;
    char* nulstr=" ";
    LONGLONG firstelem=1;

    struct meds_image_info *info=self->data;
    for (long i=0; i<self->size; i++) {
        long row = i+1;
        if (fits_read_col_str(fits, colnum, row, firstelem, 1,
                              nulstr, &info->filename, NULL, &status)) {
            fits_report_error(stderr,status);
            return 0;
        }

        info++;
    }

    return 1;
}

static struct meds_info_cat *read_image_info(fitsfile *fits)
{
    int status=0;
    if (fits_movnam_hdu(fits, BINARY_TBL, "image_info", 0, &status)) {
        fits_report_error(stderr,status);
        return NULL;
    }

    long nrows=get_nrows(fits);
    if (!nrows) {
        return NULL;
    }
    //printf("found %ld rows in image info table\n", nrows);

    long namelen_max = get_array_col_size(fits,"filename");
    //printf("namelen max: %ld\n", namelen_max);

    struct meds_info_cat *self=meds_info_cat_new(nrows, namelen_max);


    if (!load_image_info(self, fits)) {
        self=meds_info_cat_free(self);
        return NULL;
    }
    return self;
}


//
// struct meds
//

struct meds *meds_open(const char *filename)
{
    fitsfile *fits=fits_open(filename);
    if (!fits) {
        return NULL;
    }

    struct meds_cat *cat=read_object_table(fits);
    if (!cat) {
        fits=fits_close(fits);
        return NULL;
    }
    struct meds_info_cat *image_info=read_image_info(fits);
    if (!image_info) {
        fits=fits_close(fits);
        return NULL;
    }

    struct meds *self=calloc(1, sizeof(struct meds));
    if (!self) {
        fprintf(stderr,"could not allocate struct meds\n");
        exit(1);
    }

    self->filename=strdup(filename);
    self->fits=fits;
    self->cat=cat;
    self->image_info=image_info;
    return self;
}

struct meds *meds_free(struct meds *self)
{
    if (self) {
        free(self->filename);
        self->cat=meds_cat_free(self->cat);
        self->image_info=meds_info_cat_free(self->image_info);
        self->fits=fits_close(self->fits);
        free(self);
    }
    return NULL;
}


void meds_print(const struct meds *self,FILE* stream)
{
    if (self) {
        fprintf(stream,"MEDS structure\n");
        fprintf(stream,"    filename:      %s\n", self->filename);
        fprintf(stream,"    nobj:          %ld\n", self->cat->size);
        fprintf(stream,"    source images: %ld\n", self->image_info->size);
    }
}

static int check_iobj(const struct meds_cat *self, long iobj)
{
    if (iobj < 0 || iobj >= self->size) {
        fprintf(stderr,"iobj %ld out of range [0,%ld)\n", 
                iobj, self->size);
        return 0;
    } else {
        return 1;
    }
}
const struct meds_obj *meds_get_obj(const struct meds *self, long iobj)
{
    const struct meds_cat *cat=self->cat;
    if (!check_iobj(cat, iobj)) {
        return NULL;
    }
    return &cat->data[iobj];
}

/*
static int check_icutout(const struct meds_obj *self, long icutout)
{
    long ncutout=self->ncutout;
    if (icutout < 0 || icutout >= ncutout) {
        fprintf(stderr,
                "icutout %ld out of range [0,%ld)\n",
                icutout, ncutout);
        return 0;
    } else {
        return 1;
    }
}
*/

static int check_iobj_icutout(const struct meds *self, 
                              long iobj,
                              long icutout)
{
    const struct meds_obj *obj=meds_get_obj(self, iobj);
    if (!obj) {
        return 0;
    } else {
        long ncutout=obj->ncutout;
        if (icutout < 0 || icutout >= ncutout) {
            fprintf(stderr,
                    "icutout %ld out of range [0,%ld) for object %ld\n",
                    icutout, ncutout, iobj);
            return 0;
        } else {
            return 1;
        }
    }
}



long meds_get_source_file_id(const struct meds *self,
                             long iobj,
                             long icutout)
{
    if (!check_iobj_icutout(self, iobj, icutout)) {
        return -9999;
    }
    const struct meds_obj *obj=meds_get_obj(self, iobj);
    return obj->file_id[icutout];
}

const struct meds_image_info *meds_get_source_info(const struct meds *self,
                                                   long iobj,
                                                   long icutout)
{
    long file_id = meds_get_source_file_id(self, iobj, icutout);
    if (file_id < 0) {
        return NULL;
    }
    return &self->image_info->data[file_id];
}

const char *meds_get_source_filename(const struct meds *self,
                                     long iobj,
                                     long icutout)
{
    const struct meds_image_info *info=meds_get_source_info(self,iobj,icutout);
    if (!info) {
        return NULL;
    }
    return info->filename;
}

