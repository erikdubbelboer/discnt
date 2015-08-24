source "../tests/includes/init-tests.tcl"    

test "INCR propagates" {
    set e [D 1 incr 02propagate 1.0]
    after 1100
    set e [D 2 get 02propagate]
    assert {$e <  1.1}
    assert {$e >= 1.0}
    after 3000
    set e [D 1 get 02propagate]
    assert {$e eq 1}
}

test "negative INCR propagates" {
    set e [D 0 incr 02propagate -2.0]
    after 4100
    set e [D 1 get 02propagate]
    assert {$e eq -1}
}

test "5 seconds of predictions" {
    set e [D 0 incr 02propagate 1.0]
    for {set i 0} {$i < 5} {incr i} {
        after 1000
        set e [D 1 get 02propagate]
        assert {$e < 0.1 }
        assert {$e > -0.1}
    }
    set e [D 2 get 02propagate]
    assert {$e eq 0}
}
