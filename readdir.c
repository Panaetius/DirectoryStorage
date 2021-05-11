static char readdir_module_documentation[] =
"scalable os.listdir\n"
"\n"
;

#include "Python.h"
#include <dirent.h>
#include <errno.h>

typedef struct {
     PyObject_HEAD
     DIR *dir;
} dirobject;

static void dir_dealloc(dirobject *self)
{
    Py_BEGIN_ALLOW_THREADS
    closedir(self->dir);
    Py_END_ALLOW_THREADS
    PyObject_Del(self);
}

static PyObject *dir_read(dirobject *self, PyObject *args)
{
    struct dirent *d;
    if(!(PyArg_ParseTuple(args, "")))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    d = readdir(self->dir);
    Py_END_ALLOW_THREADS

    if(!d)
    {
        if(errno)
        {
            PyErr_SetFromErrno(PyExc_IOError);
            return NULL;
        }
        else
        {
            return PyString_FromString("");
        }
    }

    return PyString_FromString(d->d_name);
}

static struct PyMethodDef dir_methods[] = {
  {"read",         (PyCFunction)dir_read,   1,
   "read() -- read the next directory entry"
  },
  {NULL,              NULL}           /* sentinel */
};

static PyObject *dir_getattr(dirobject *self, char *name)
{
    return Py_FindMethod(dir_methods, (PyObject *)self, name);
}

static char dir_doc [] = "incremental directory reader";

static PyTypeObject dirtype = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size*/
    "readdir",               /* tp_name*/
    sizeof(dirobject),       /* tp_basicsize*/
    0,                       /* tp_itemsize*/
    /* methods */
    (destructor)dir_dealloc,/* tp_dealloc*/
    (printfunc)0,            /* tp_print*/
    (getattrfunc)dir_getattr,/* tp_getattr*/
    (setattrfunc)0,          /* tp_setattr*/
    (cmpfunc)0,              /* tp_compare*/
    (reprfunc)0,             /* tp_repr*/
    0,                       /* tp_as_number*/
    0,                       /* tp_as_sequence*/
    0,                       /* tp_as_mapping*/
    (hashfunc)0,             /* tp_hash*/
    (ternaryfunc)0,          /* tp_call*/
    (reprfunc)0,             /* tp_str*/
    0,                       /* tp_getattro */
    0,                       /* tp_setattro */
    0,                       /* tp_as _buffer*/
    Py_TPFLAGS_DEFAULT,      /* tp_flags */
    dir_doc,                 /* tp_doc */
    0,                       /* tp_traverse */
    0,                       /* tp_clear */
    0,                       /* tp_richcompare */
};

static dirobject *newdirobject(DIR *dir)
{
    dirobject *self;

    if(!(self = PyObject_NEW(dirobject, &dirtype)))
    {
        Py_BEGIN_ALLOW_THREADS
        closedir(dir);
        Py_END_ALLOW_THREADS
        return NULL;
    }

    self->dir = dir;

    return self;
}


static PyObject *p_opendir(PyObject *self, PyObject *args)
{
    const char *name;
    DIR *dir;

    if(!(PyArg_ParseTuple(args, "s", &name)))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    dir = opendir(name);
    Py_END_ALLOW_THREADS

    if(!dir)
    {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
    return (PyObject *)newdirobject(dir);
}

static struct PyMethodDef readdir_attrs[] = {
    {"opendir",    (PyCFunction)p_opendir,    1,
     "opendir(name) -- open a directory for reading"},
    { NULL, NULL }
};

#ifndef DL_EXPORT/* declarations for DLL import/export */
#define DL_EXPORT(RTYPE) RTYPE
#endif
DL_EXPORT(void) initreaddir() {
    dirtype.ob_type = &PyType_Type;
    Py_InitModule3("readdir", readdir_attrs,
                       readdir_module_documentation);
}
