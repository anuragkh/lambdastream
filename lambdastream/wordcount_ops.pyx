#!python
# cython: profile=True
# cython: linetrace=True
# cython: embedsignature=True, binding=True
# distutils: language = c++
# cython: language_level = 3
# distutils: define_macros=CYTHON_TRACE_NOGIL=1

from cpython.ref cimport PyObject
from cpython.dict cimport PyDict_GetItem

def cython_process_batch_map(batch, int num_reducers):
    cdef:
        int h

    keyed_words = [[] for key in range(num_reducers)]
    for row in batch:
        for word in row.split(b' '):
            h = hash(word)
            h = h % num_reducers
            keyed_words[h].append(word)

    for reducer in range(num_reducers):
        keyed_words[reducer] = [b" ".join(keyed_words[reducer])]

    return keyed_words

cpdef cython_process_batch_reduce(dict state, bytes words):
    cdef PyObject *obj
    cdef Py_ssize_t val
    for word in words.split(b' '):
        obj = PyDict_GetItem(state, word)
        if obj is NULL:
            state[word] = 1
        else:
            val = <object> obj
            state[word] = val + 1
    return word
