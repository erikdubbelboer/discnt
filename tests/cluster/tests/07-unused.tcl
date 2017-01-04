source "../tests/includes/init-tests.tcl"    

test "0 counters should be deleted" {
    D 0 incrby test1 1
    D 0 set test1 0

    # Wait for one sync+delete round.
    after 2100

    for {set i 0} {$i < $::instances_count} {incr i} {
        set v [D $i exists test1]
        assert {$v == 0}
        set v [D $i get test1 state]
        assert {$v == "0 CONSISTENT"}
    }
}

test "0 shards should be deleted" {
    D 0 set test2 1
    # Wait for one sync round.
    after 1100
    D 1 set test2 0
    # Wait for one sync round.
    after 1100
    D 2 set test2 1
    # Wait for one sync round.
    after 1100

    # 0 = 1
    # 1 = -1
    # 2 = 1
    # so the counter value = 1

    set v [D 1 debug counter test2]
    assert {[llength [lindex $v 0]] == 3}

    D 0 set test2 0

    # Wait for one sync+delete round.
    after 2100

    for {set i 0} {$i < $::instances_count} {incr i} {
        set v [D $i debug counter test2]
        assert {[llength [lindex $v 0]] == 2}
    }
}

test "delete with subscribers" {
    D 0 set test3 1

    D 1 deferred 1
    D 1 subscribe test3
    D 1 read; # Read the subscribe reply

    # Wait for one sync round.
    after 1100

    D 0 set test3 0

    # Wait for one sync+delete round.
    after 2100

    set v0 [D 0 exists test3]
    assert {$v0 == 0}

    D 1 read; # Read the change to 1
    D 1 read; # Read the change to 0

    D 1 unsubscribe test3
    D 1 read; # Read the unsubscribe reply
    D 1 deferred 0
    set v1 [D 1 exists test3]
    assert {$v1 == 1}
}
