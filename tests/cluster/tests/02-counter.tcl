source "../tests/includes/init-tests.tcl"    

test "INCR a counter" {
    set v [D 0 incr 02incget 1.0]
    set v [D 0 incr 02incget 1.0]
    assert {$v eq 2}
}

test "GET a counter" {
    set v [D 0 get 02incget]
    assert {$v eq 2}
}

test "GET on non existent counter should return 0" {
    set v [D 0 get doesnotexist]
    assert {$v eq 0}
}
