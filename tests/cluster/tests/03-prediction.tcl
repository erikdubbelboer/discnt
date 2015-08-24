source "../tests/includes/init-tests.tcl"    

test "INCR should propagate" {
    set v [D 1 incr 02propagate 1.0]
    after 1100
    set v [D 2 get 02propagate]
    assert {$v <  1.1}
    assert {$v >= 1.0}
    after 3000
    set v [D 2 get 02propagate]
    assert {$v eq 1}
}

test "Negative INCR should propagate" {
    set v [D 0 incr 02propagate -2.0]
    after 4100
    set v [D 1 get 02propagate]
    assert {$v eq -1}
}

