source "../tests/includes/init-tests.tcl"    

test "INCR propagates" {
    set a [D 1 incr 02propagate 1.0]
    after 1100
    set b [D 2 get 02propagate]
    assert {$b <  1.1}
    assert {$b >= 1.0}
    after 3000
    set c [D 2 get 02propagate]
    assert {$c eq 1}
}

test "negative INCR propagates" {
    set d [D 0 incr 02propagate -2.0]
    after 4100
    set e [D 1 get 02propagate]
    assert {$e eq -1}
}

test "5 seconds of predictions" {
    set f [D 0 incr 02propagate 1.0]

    # Wait a bit so our first get will be after 1100ms.
    after 100

    for {set i 0} {$i < 5} {incr i} {
        after 1000
        set g [D 1 get 02propagate]
        assert {$g < 0.1 }
        assert {$g > -0.1}
    }
    set h [D 2 get 02propagate]
    assert {$h eq 0}
}
