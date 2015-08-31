source "../tests/includes/init-tests.tcl"    

test "INCR should propagate" {
    D 0 incrby test1 2
    D 0 set test2 2

    # Wait for one sync round.
    after 1100

    set v [D 1 get test1]
    assert {$v <= 4}
    assert {$v >= 2}

    set v [D 1 get test2]
    assert {$v == 2}
}

test "DECR should propagate" {
    D 0 decrby test1 4.0
    D 0 set test2 1

    # Wait for one sync round.
    after 1100

    set v [D 1 get test1]
    assert {$v <=  0}
    assert {$v >= -3}

    set v [D 1 get test2]
    assert {$v == 1}
}

