source "../tests/includes/init-tests.tcl"

test "Killing a node" {
    kill_instance discnt 2
}

test "Predictions will not ack" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 1
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }

    set e [D 1 incr test 1.0]
}

test "Restarting node" {
    restart_instance discnt 2
}

test "Prediction should be resend" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }

    after 1100

    set e [D 2 get test]
    assert {$e <= 1.25}
    assert {$e >= 1.0 }
}
