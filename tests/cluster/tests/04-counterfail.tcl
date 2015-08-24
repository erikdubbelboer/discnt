source "../tests/includes/init-tests.tcl"

test "Killing a node" {
    set e [D 2 incr test 1.0]

    after 1500

    # After 1.5 seconds it can't be higher than
    # 1.0 + (1.0 / 4.0) = 1.25
    set e [D 1 get test]
    assert {$e <= 1.25}
    assert {$e >= 1.0}

    kill_instance discnt 2
}

test "Predictions should stop" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 1
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }

    set e [D 1 get test]
    assert {$e <  4.0}
    assert {$e >= 1.0}
}

test "Restarting node" {
    restart_instance discnt 2
}

test "Prediction should restore" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }

    set e [D 1 get test]
    assert {$e eq 1}
}
