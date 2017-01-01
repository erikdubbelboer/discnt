start_server {tags {"counters"}} {
    test {INCR a counter} {
        r incr test
        r incrby test 2
    } {3}

    test {GET a counter} {
        r get test
    } {3}

    test {GET with STATE} {
        r get test state
    } {3 CONSISTENT}

    test {GET on non existent counter should return 0} {
        r get doesnotexist
    } {0}

    test {MGET} {
        r incr test2
        r mget test doesnotexist test2
    } {3 0 1}

    test {KEYS should return all counters} {
        lsort [r keys *]
    } {test test2}

    test {PRECISION on none existing counter} {
        r precision nonexisting
    } {1}

    test {PRECISION} {
        r precision test 2.2
        set _ [expr {round([r precision test] * 10)}]
    } {22}

    test {CONFIG SET default-precision} {
        r config set default-precision 0.1
        set _ [expr {round([r precision test3] * 10)}]
    } {1}

    test {SET} {
        r set test 1
    } {1}

    test {GET after SET} {
        r get test
    } {1}

    # The following test can only be executed if we don't use Valgrind, and if
    # we are using x86_64 architecture, because:
    #
    # 1) Valgrind has floating point limitations, no support for 80 bits math.
    # 2) Other archs may have the same limits.
    #
    # 1.23 cannot be represented correctly with 64 bit doubles, so we skip
    # the test, since we are only testing pretty printing here and is not
    # a bug if the program outputs things like 1.299999...
    if {!$::valgrind || ![string match *x86_64* [exec uname -a]]} {
        test {Test counters for correct float representation} {
            assert {[r incrbyfloat test3 1.23] eq {1.23}}
            assert {[r incrbyfloat test3 0.77] eq {2}}
            assert {[r incrbyfloat test3 -0.1] eq {1.9}}
        }
    }

    test {EXISTS} {
        r exists test test2
    } {2}
}
