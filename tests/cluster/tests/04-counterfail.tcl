source "../tests/includes/init-tests.tcl"

test "Killing a node" {
    set v [D 2 incr test 1.0]

    after 1500

    # After 1.5 seconds it can't be higher than
    # 1.0 + (1.0 / 4.0) = 1.25
    set v [D 1 get test]
    assert {$v <= 1.25}
    assert {$v >= 1.0}

    kill_instance discnt 2
}

test "Predictions should stop" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 1
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }

    set v [D 1 get test]
    assert {$v <  4.0}
    assert {$v >= 1.0}
}

test "Restarting node" {
    restart_instance discnt 2
}

test "Prediction should be restored" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }

    set v [D 1 get test]
    assert {$v eq 1}
}
