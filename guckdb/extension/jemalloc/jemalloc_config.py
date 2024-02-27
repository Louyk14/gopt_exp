import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/')) for x in ['extension/jemalloc/include', 'extension/jemalloc/jemalloc/include']
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/jemalloc/jemalloc_extension.cpp',
        'extension/jemalloc/jemalloc/src/arena.cpp',
        'extension/jemalloc/jemalloc/src/background_thread.cpp',
        'extension/jemalloc/jemalloc/src/base.cpp',
        'extension/jemalloc/jemalloc/src/bin.cpp',
        'extension/jemalloc/jemalloc/src/bin_info.cpp',
        'extension/jemalloc/jemalloc/src/bitmap.cpp',
        'extension/jemalloc/jemalloc/src/buf_writer.cpp',
        'extension/jemalloc/jemalloc/src/cache_bin.cpp',
        'extension/jemalloc/jemalloc/src/ckh.cpp',
        'extension/jemalloc/jemalloc/src/counter.cpp',
        'extension/jemalloc/jemalloc/src/ctl.cpp',
        'extension/jemalloc/jemalloc/src/decay.cpp',
        'extension/jemalloc/jemalloc/src/div.cpp',
        'extension/jemalloc/jemalloc/src/ecache.cpp',
        'extension/jemalloc/jemalloc/src/edata.cpp',
        'extension/jemalloc/jemalloc/src/edata_cache.cpp',
        'extension/jemalloc/jemalloc/src/ehooks.cpp',
        'extension/jemalloc/jemalloc/src/emap.cpp',
        'extension/jemalloc/jemalloc/src/eset.cpp',
        'extension/jemalloc/jemalloc/src/exp_grow.cpp',
        'extension/jemalloc/jemalloc/src/extent.cpp',
        'extension/jemalloc/jemalloc/src/extent_dss.cpp',
        'extension/jemalloc/jemalloc/src/extent_mmap.cpp',
        'extension/jemalloc/jemalloc/src/fxp.cpp',
        'extension/jemalloc/jemalloc/src/hook.cpp',
        'extension/jemalloc/jemalloc/src/hpa.cpp',
        'extension/jemalloc/jemalloc/src/hpa_hooks.cpp',
        'extension/jemalloc/jemalloc/src/hpdata.cpp',
        'extension/jemalloc/jemalloc/src/inspect.cpp',
        'extension/jemalloc/jemalloc/src/jemalloc.cpp',
        'extension/jemalloc/jemalloc/src/large.cpp',
        'extension/jemalloc/jemalloc/src/log.cpp',
        'extension/jemalloc/jemalloc/src/malloc_io.cpp',
        'extension/jemalloc/jemalloc/src/mutex.cpp',
        'extension/jemalloc/jemalloc/src/nstime.cpp',
        'extension/jemalloc/jemalloc/src/pa.cpp',
        'extension/jemalloc/jemalloc/src/pa_extra.cpp',
        'extension/jemalloc/jemalloc/src/pac.cpp',
        'extension/jemalloc/jemalloc/src/pages.cpp',
        'extension/jemalloc/jemalloc/src/pai.cpp',
        'extension/jemalloc/jemalloc/src/peak_event.cpp',
        'extension/jemalloc/jemalloc/src/prof.cpp',
        'extension/jemalloc/jemalloc/src/prof_data.cpp',
        'extension/jemalloc/jemalloc/src/prof_log.cpp',
        'extension/jemalloc/jemalloc/src/prof_recent.cpp',
        'extension/jemalloc/jemalloc/src/prof_stats.cpp',
        'extension/jemalloc/jemalloc/src/prof_sys.cpp',
        'extension/jemalloc/jemalloc/src/psset.cpp',
        'extension/jemalloc/jemalloc/src/rtree.cpp',
        'extension/jemalloc/jemalloc/src/safety_check.cpp',
        'extension/jemalloc/jemalloc/src/san.cpp',
        'extension/jemalloc/jemalloc/src/san_bump.cpp',
        'extension/jemalloc/jemalloc/src/sc.cpp',
        'extension/jemalloc/jemalloc/src/sec.cpp',
        'extension/jemalloc/jemalloc/src/stats.cpp',
        'extension/jemalloc/jemalloc/src/sz.cpp',
        'extension/jemalloc/jemalloc/src/tcache.cpp',
        'extension/jemalloc/jemalloc/src/test_hooks.cpp',
        'extension/jemalloc/jemalloc/src/thread_event.cpp',
        'extension/jemalloc/jemalloc/src/ticker.cpp',
        'extension/jemalloc/jemalloc/src/tsd.cpp',
        'extension/jemalloc/jemalloc/src/witness.cpp',
    ]
]