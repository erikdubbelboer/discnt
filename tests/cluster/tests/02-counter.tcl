source "../tests/includes/init-tests.tcl"    

test "INCR and GET a counter" {
    set e [D 0 incr mycounter 1]
    set e [D 0 incr mycounter 1]
    after 1100
    set g [D 0 get mycounter]
    assert {$e eq 2}
    assert {$e eq $g}
}

test "GET on non existent counter" {
    set g [D 0 get doesnotexist]
    assert {$g eq 0}
}

test "INCR propagates" {
    set e [D 0 incr propagatecounter1 3]
    assert {$e < 3.4}
    assert {$e > 2.6}
    after 3000
    set e [D 1 get propagatecounter1]
    assert {$e < 3.4}
    assert {$e > 2.6}
    after 2000
    set e [D 1 get propagatecounter1]
    assert {$e < 3.4}
    assert {$e > 2.6}
}

test "negative INCR propagates" {
    set e [D 0 incr propagatecounter2 1]
    after 1100
    set e [D 0 incr propagatecounter2 -1]
    after 2000
    set e [D 1 get propagatecounter2]
    assert {$e < 0.2}
    assert {$e > -0.2}
}

test "12 second propagates" {
    set e [D 0 incr propagatecounter3 1]
    after 2000
    for {set i 0} {$i < 12} {incr i} {
        after 1000
        set e [D 1 get propagatecounter3]
        assert {$e < 1.2}
        assert {$e > 0.8}
    }
}
