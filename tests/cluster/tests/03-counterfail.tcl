source "../tests/includes/init-tests.tcl"

test "Killing a node" {
    set v [D 2 incr test]

    after 1100

    kill_instance discnt 2
}

test "Predictions should stop" {
    global lastvalue

    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 1
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }

    set lastvalue [D 1 get test]
}

test "Restarting node" {
    restart_instance discnt 2
}

test "Prediction should be restored" {
    global lastvalue

    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }

    # The prediction should not have changed.
    set v [D 1 get test]
    assert {$v == $lastvalue}
    
    after 1100

    # Even after a next round of updates when the node is up again.
    set v [D 1 get test]
    assert {$v == $lastvalue}
}
