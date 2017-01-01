source "../tests/includes/init-tests.tcl"    

test "0 counters should be deleted" {
    D 0 incrby test 1
    D 0 set test 0

    # Wait for one sync+delete round.
    after 2100

    for {set i 0} {$i < $::instances_count} {incr i} {
        set v [D $i exists test]
        assert {$v == 0}
        set v [D $i get test state]
        assert {$v == "0 CONSISTENT"}
    }
}

test "0 shards should be deleted" {
    D 0 set test 1
    # Wait for one sync round.
    after 1100
    D 1 set test 0
    # Wait for one sync round.
    after 1100
    D 2 set test 1
    # Wait for one sync round.
    after 1100

    # 0 = 1
    # 1 = -1
    # 2 = 1
    # so the counter value = 1

    set v [D 1 debug counter test]
    assert {[llength [lindex $v 0]] == 3}

    D 0 set test 0

    # Wait for one sync+delete round.
    after 2100

    for {set i 0} {$i < $::instances_count} {incr i} {
        set v [D $i debug counter test]
        assert {[llength [lindex $v 0]] == 2}
    }
}
