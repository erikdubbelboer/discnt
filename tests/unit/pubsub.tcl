start_server {tags {"pubsub"}} {
    proc __consume_subscribe_messages {client type counters} {
        set numsub -1
        set counts {}

        for {set i [llength $counters]} {$i > 0} {incr i -1} {
            set msg [$client read]
            assert_equal $type [lindex $msg 0]

            # when receiving subscribe messages the counter names
            # are ordered. when receiving unsubscribe messages
            # they are unordered
            set idx [lsearch -exact $counters [lindex $msg 1]]
            if {[string match "*unsubscribe" $type]} {
                assert {$idx >= 0}
            } else {
                assert {$idx == 0}
            }
            set counters [lreplace $counters $idx $idx]

            # aggregate the subscription count to return to the caller
            lappend counts [lindex $msg 2]
        }

        # we should have received messages for counters
        assert {[llength $counters] == 0}
        return $counts
    }

    proc subscribe {client counters} {
        $client subscribe {*}$counters
        __consume_subscribe_messages $client subscribe $counters
    }

    proc unsubscribe {client {counters {}}} {
        $client unsubscribe {*}$counters
        __consume_subscribe_messages $client unsubscribe $counters
    }

    test "Sub PING" {
        set rd1 [redis_deferring_client]
        subscribe $rd1 somecounter
        # While subscribed to a counter PING works in Sub mode.
        $rd1 ping
        $rd1 ping "foo"
        set reply1 [$rd1 read]
        set reply2 [$rd1 read]
        unsubscribe $rd1 somecounter
        # Now we are unsubscribed, PING should just return PONG.
        $rd1 ping
        set reply3 [$rd1 read]
        $rd1 close
        list $reply1 $reply2 $reply3
    } {{pong {}} {pong foo} PONG}

    test "SUBSCRIBE basics" {
        set rd1 [redis_deferring_client]

        # subscribe to two counters
        assert_equal {1 2} [subscribe $rd1 {test1 test2}]
        r set test1 1
        r set test2 2
        after 1100
        set v [lsort [list [$rd1 read] [$rd1 read]]]
        assert_equal {{message test1 1} {message test2 2}} $v

        # unsubscribe from one of the channels
        unsubscribe $rd1 {test1}
        r set test2 3
        after 1100
        assert_equal {message test2 3} [$rd1 read]

        # unsubscribe from the remaining channel
        unsubscribe $rd1 {test2}

        # clean up clients
        $rd1 close
    }
}
