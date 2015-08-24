source "../tests/includes/init-tests.tcl"    

test "INCR a counter" {
    set v [D 0 incr test 1]
    set v [D 0 incr test 1]
    assert {$v eq 2}
}

test "GET a counter" {
    set v [D 0 get test]
    assert {$v eq 2}
}

test "GET on non existent counter should return 0" {
    set v [D 0 get doesnotexist]
    assert {$v eq 0}
}

test "COUNTERS should return all counters" {
    D 0 incr test2 1

    set v [lsort [D 0 counters *]]
    assert {$v eq {test test2}}
}
