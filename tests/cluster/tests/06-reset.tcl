source "../tests/includes/init-tests.tcl"

test "Reset should reset" {
    D 1 set test 1
    D 2 incrby test 10

    after 5000

    D 3 debug reset

    after 1000

    for {set i 0} {$i < $::instances_count} {incr i} {
        set v [D $i keys *]
        assert {$v == {}}
    }
}
