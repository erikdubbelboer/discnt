This directory contains all Discnt dependencies, except for the libc that
should be provided by the operating system.

* **Jemalloc** is our memory allocator, used as replacement for libc malloc on Linux by default. It has good performances and excellent fragmentation behavior. This component is upgraded from time to time.
* **hiredis** is the official C client library for Discnt. It is used by discnt-cli and discnt-benchmark. It is part of the Discnt official ecosystem but is developed externally from the Discnt repository, so we just upgrade it as needed.
* **linenoise** is a readline replacement. It is developed by the same authors of Discnt but is managed as a separated project and updated as needed.

How to upgrade the above dependencies
===

Jemalloc
---

Jemalloc is unmodified. We only change settings via the `configure` script of Jemalloc using the `--with-lg-quantum` option, setting it to the value of 3 instead of 4. This provides us with more size classes that better suit the Discnt data structures, in order to gain memory efficiency.

So in order to upgrade jemalloc:

1. Remove the jemalloc directory.
2. Substitute it with the new jemalloc source tree.

Hiredis
---

Hiredis uses the SDS string library, that must be the same version used inside Discnt itself. Historically Discnt often used forked versions of hiredis in a way or the other. In order to upgrade it is adviced to take a lot of care:

1. Check with diff if hiredis API changed and what impact it could have in Discnt.
2. Make sure thet the SDS library inside Hiredis and inside Discnt are compatible.
3. After the upgrade, run the Discnt Sentinel test.
4. Check manually that discnt-cli and discnt-benchmark behave as expecteed, since we have no tests for CLI utilities currently.

Linenoise
---

Linenoise is rarely upgraded as needed. The upgrade process is trivial since
Discnt uses a non modified version of linenoise, so to upgrade just do the
following:

1. Remove the linenoise directory.
2. Substitute it with the new linenoise source tree.

