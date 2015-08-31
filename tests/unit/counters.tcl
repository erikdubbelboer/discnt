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

    test {KEYS should return all counters} {
        r incr test2
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

    test {GET aftet SET} {
        r get test
    } {1}
}
