source "../tests/includes/init-tests.tcl"    

test "INCR and GET a counter" {
    set e [D 0 incr 02incget 1.0]
    set e [D 0 incr 02incget 1.0]
    assert {$e eq 2}
    set g [D 0 get 02incget]
    assert {$e eq $g}
}

test "GET on non existent counter" {
    set g [D 0 get doesnotexist]
    assert {$g eq 0}
}
