source "../tests/includes/init-tests.tcl"    

test "Increment for 1 minute" {
    # 10 seconds
    for {set i 0} {$i < 100} {incr i} {
        after 100
        D 0 incr test0 0.01
        D 1 incr test1 0.1
        D 2 incr test2 1.0
        D 3 incr test3 0.8
        D 4 incr test3 1.0
        D 5 incr test3 -1.2
    }

    kill_instance discnt 6

    # 20 seconds
    for {set i 0} {$i < 200} {incr i} {
        after 100
        D 0 incr test0 0.01
        D 1 incr test1 0.1
        D 2 incr test2 1.0
        D 3 incr test3 0.8
        D 4 incr test3 1.0
        D 5 incr test3 -1.2
    }

    restart_instance discnt 6

    # 30 seconds
    for {set i 0} {$i < 300} {incr i} {
        after 100
        D 0 incr test0 0.01
        D 1 incr test1 0.1
        D 2 incr test2 1.0
        D 3 incr test3 0.8
        D 4 incr test3 1.0
        D 5 incr test3 -1.2
    }
}

test "Accurate counters" {
    set v [expr {round([D 0 get test0])}]
    assert {$v == 6} ; # 6 = 600 * 0.01

    set v [expr {round([D 1 get test1])}]
    assert {$v == 60}

    set v [expr {round([D 2 get test2])}]
    assert {$v == 600}
}

test "Waiting for history" {
    # It takes 1/(0.8/4)= 5 seconds before test3 on instance 3's
    # prediction will be 1 off from the actual value.
    # Add 2 extra seconds of tolarance for the prediction updates
    # and syncs.
    after 7000
}

for {set i 0} {$i < $::instances_count} {incr i} {
    test "Counters on insance $i" {
        set v [expr {round([D $i get test0])}]
        # Use a tolerance of 1 instead of the actual really low value.
        # If we have to wait for a counter with this low increment to
        # be accurate we have to wait a long time.
        assert {$v >= 5} ; # 5 = 600*0.01 - 1
        assert {$v <= 7} ; # 7 = 600*0.01 + 1

        set v [expr {round([D $i get test1])}]
        assert {$v >= 59}
        assert {$v <= 71}

        set v [expr {round([D $i get test2])}]
        assert {$v >= 590}
        assert {$v <= 710}

        set v [expr {round([D $i get test3])}]
        assert {$v >= 357} ; # 357 = 600*(0.8+1.0-1.2) + -1-1-1
        assert {$v <= 363} ; # 363 = 600*(0.1+0.2-0.01) + 1+1+1
    }
}

for {set i 0} {$i < $::instances_count} {incr i} {
    test "Instance $i \[Hits, Misses\]" {
        set h [DI $i hits]
        set m [DI $i misses]
        puts -nonewline "\[$h, $m\] "
    }
}

