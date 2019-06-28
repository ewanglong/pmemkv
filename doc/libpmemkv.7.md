---
layout: manual
Content-Style: 'text/css'
title: _MP(PMEMKV, 7)
collection: libpmemkv
header: PMEMKV
date: pmemkv version 0.8
...

[comment]: <> (Copyright 2019, Intel Corporation)

[comment]: <> (Redistribution and use in source and binary forms, with or without)
[comment]: <> (modification, are permitted provided that the following conditions)
[comment]: <> (are met:)
[comment]: <> (    * Redistributions of source code must retain the above copyright)
[comment]: <> (      notice, this list of conditions and the following disclaimer.)
[comment]: <> (    * Redistributions in binary form must reproduce the above copyright)
[comment]: <> (      notice, this list of conditions and the following disclaimer in)
[comment]: <> (      the documentation and/or other materials provided with the)
[comment]: <> (      distribution.)
[comment]: <> (    * Neither the name of the copyright holder nor the names of its)
[comment]: <> (      contributors may be used to endorse or promote products derived)
[comment]: <> (      from this software without specific prior written permission.)

[comment]: <> (THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS)
[comment]: <> ("AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT)
[comment]: <> (LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR)
[comment]: <> (A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT)
[comment]: <> (OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,)
[comment]: <> (SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT)
[comment]: <> (LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,)
[comment]: <> (DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY)
[comment]: <> (THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT)
[comment]: <> ((INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE)
[comment]: <> (OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.)

[comment]: <> (libpmemkv.7 -- man page for libpmemkv)

[NAME](#name)<br />
[DESCRIPTION](#description)<br />
[ENGINES](#engines)<br />
[BINDINGS](#bindings)<br />
[SEE ALSO](#see-also)<br />


# NAME #

**pmemkv** - Key/Value Datastore for Persistent Memory

# DESCRIPTION #

**pmemkv** is a key-value datastore framework optimized for persistent memory. It provides native C API and C++ headers. Support for other languages is described in the **BINDINGS** section below.

It has multiple storage engines, each optimized for a different use case. They differ in implementation and capabilities:

+ persistence - this is a trade-off between data preservation and performance; persistent engines retain their content and are power fail/crash safe, but are slower; volatile engines are faster, but keep their content only until the database is closed (or application crashes; power fail occurs)

+ concurrency - engines provide a varying degree of write scalability in multi-threaded workloads. Concurrent engines support non-blocking retrievals and, on average, highly scalable updates. For details see the description of individual engines.

+ keys' ordering - "sorted" engines support querying above/below the given key

Persistent engines usually use libpmemobj++ and PMDK to access NVDIMMs. They can work with files on DAX filesystem (fsdax) or DAX device.

For description of pmemkv core API see **libpmemkv**(3).
For description of pmemkv configuration API see **libpmemkv_config**(3).

# ENGINES #

| Engine Name  | Description | Persistent? | Concurrent? | Sorted? |
| ------------ | ----------- | ----------- | ----------- | ------- |
| **cmap** | **Concurrent hash map** | **Yes** | **Yes** | **No** |
| vcmap | Volatile concurrent hash map | No | Yes | No |
| vsmap | Volatile sorted hash map | No | No | Yes |
| blackhole | Accepts everything, returns nothing | No | Yes | No |

The most mature and recommended engine to use for persistent use-cases is **cmap**. It provides good performance results and stability.

Each engine can be manually turned on and off at build time, using CMake options. All engines listed here are enabled and ready to use.

## cmap

A persistent concurrent engine, backed by a hashmap that allows gets, puts, and removes concurrently from multiple threads.
Internally this engine uses persistent concurrent hashmap and persistent string from libpmemobj-cpp library (for details see <https://github.com/pmem/libpmemobj-cpp>). Persistent string is used as a type of a key and a value.
TBB and libpmemobj-cpp packages are required.
Supports path, size, and force_create configuration parameters.

## vcmap

A volatile concurrent engine, backed by memkind.
This engine is built on top of tbb::concurrent\_hash\_map data structure and uses PMEM C++ allocator to allocate memory. std::basic\_string is used as a type of a key and a value.
Memkind, TBB and libpmemobj-cpp packages are required.
Supports path and size configuration parameters.

## vsmap

A volatile single-threaded sorted engine, backed by memkind.
This engine is built on top of std::map and uses PMEM C++ allocator to allocate memory. std::basic\_string is used as a type of a key and a value.
Memkind and libpmemobj-cpp packages are required.
Supports path and size configuration parameters.


## blackhole

A volatile engine that accepts an unlimited amount of data, but never returns anything.
Internally, `blackhole` does not use a persistent pool or any durable structure. The intended use of this engine is to profile and tune high-level bindings, and similar cases when persistence
should be intentionally skipped.
No additional packages are required.
No supported configuration parameters.

### Experimental engines

There are also more engines in various states of development, for details see <https://github.com/pmem/pmemkv>.

# BINDINGS #

Bindings for other languages are available on GitHub. Currently they support only subset of native API.

Existing bindings:

+ Ruby - for details see <https://github.com/pmem/pmemkv-ruby>

+ JNI & Java - for details see <https://github.com/pmem/pmemkv-java>

+ Node.js - for details see <https://github.com/pmem/pmemkv-nodejs>

# SEE ALSO #

**libpmemkv**(3), **libpmemkv_config(3)**, **pmempool**(1), **libpmemobj**(7) and **<http://pmem.io>**