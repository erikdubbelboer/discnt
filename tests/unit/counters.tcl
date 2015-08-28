start_server {tags {"counters"}} {
    test {INCR a counter} {
        r incr test 1
        r incr test 1
    } {2}

    test {GET a counter} {
        r get test
    } {2}

    test {GET on non existent counter should return 0} {
        r get doesnotexist
    } {0}

    test {COUNTERS should return all counters} {
        r incr test2 1
        lsort [r counters *]
    } {test test2}

    test {PRECISION on none existing counter} {
        r precision nonexisting
    } {1}

    test {PRECISION} {
        r precision test 2.2
        set _ [expr {round([r precision test] * 10)}]
    } {22}
}
