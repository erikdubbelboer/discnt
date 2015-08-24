source "../tests/includes/init-tests.tcl"    

test "Test 6 seconds of predictions" {
    set v [D 0 incr test 1.0]

    # Wait a bit so our first get will be after 1100ms.
    after 100

    for {set i 0} {$i < 5} {incr i} {
        after 1000
        set v [D 1 get test]
        assert {$v <  1.1}
        assert {$v >= 1.0}
    }
}

test "Counters should be precise after history of no change" {
    set v [D 1 get test]
    assert {$v eq 1}
}

test "Counter should have 0 hits" {
    set v [D 0 debug hits test]
    assert {$v eq 0}
}

test "Counter should have 4 misses" {
    set v [D 0 debug misses test]
    assert {$v eq 4}
}
