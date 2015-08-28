source "../tests/includes/init-tests.tcl"    

test "INCR should propagate" {
    D 0 incrby test 2.0

    # Wait for one sync round.
    after 1100

    set v [D 1 get test]
    assert {$v <= 4}
    assert {$v >= 2}
}

test "DECR should propagate" {
    D 0 decrby test 4.0

    # Wait for one sync round.
    after 1100

    set v [D 1 get test]
    assert {$v <=  0}
    assert {$v >= -2}
}

