source "../tests/includes/init-tests.tcl"

test "Killing a node" {
    D 1 set test 1

    kill_instance discnt 2

    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 1
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }

    # State is updated every 100ms so wait a bit.
    after 110
}

test "State should be inconsistent" {
    set v [D 1 get test state]
    assert {$v == "1 INCONSISTENT"}

    set v [D 1 get nonexistent state]
    assert {$v == "0 INCONSISTENT"}
}

test "Restarting node" {
    restart_instance discnt 2

    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }

    # State is updated every 100ms so wait a bit.
    after 110
}

test "State should be consistent" {
    set v [D 1 get test state]
    assert {$v == "1 CONSISTENT"}

    set v [D 1 get nonexistent state]
    assert {$v == "0 CONSISTENT"}
}
