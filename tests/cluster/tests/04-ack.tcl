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

    D 1 incr test1
    D 1 set test2 2
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

    # If we don't receive an ack for a prediction we wait
    # 5 seconds before we send it again. So wait at last this time.
    after 6100

    set v [D 2 get test1]
    assert {$v <= 4}
    assert {$v >= 1}

    set v [D 2 get test2]
    assert {$v == 2}
}
