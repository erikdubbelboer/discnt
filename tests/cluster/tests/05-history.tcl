source "../tests/includes/init-tests.tcl"    

test "Big counters should be accurate after a full history" {
    D 1 incrby big 10
    D 2 incrby big 20

    # Wait for a full history + one sync + 1 second tolerance.
    after 6000

    set v [expr {round([D 3 debug prediction big])}]
    assert {$v == 0}

    # After a full history of no change the counter
    # should exactly match.
    set v [D 3 get big]
    assert {$v == 30}
}

test "Small counters should be accurate after a while" {
    D 1 incrby small 0.8

    # It takes 1/(0.8/4)= 5 seconds before the counter's prediction
    # will be 1 off from the actual value.
    # Add 2 extra seconds of tolarance for the prediction updates
    # and syncs.
    after 7000

    set v [expr {round([D 3 debug prediction small])}]
    assert {$v == 0}

    # After a full history of no change the counter
    # should exactly match because the difference between
    # 1 and 0 is equal to what we allow as change.
    set v [expr {double(round([D 3 get small]*10))/10}]
    assert {$v == 0.8}
}

test "Test for issue #1" {
    D 1 incrby issue1 0.0001
    after 5000
    set v [expr {double(round([D 2 get issue1]*10000))}]
    assert {$v == 1.0}
    D 1 incrby issue1 0.0001
    after 5000
    set v [expr {double(round([D 2 get issue1]*10000))}]
    assert {$v == 2.0}
}

