start_server {tags {"other"}} {
    if {$::force_failure} {
        # This is used just for test suite development purposes.
        test {Failing test} {
            format err
        } {ok}
    }

    test {BGSAVE} {
        waitForBgsave r
        r debug flushall
        r save
        r incrby x 10
        r bgsave
        waitForBgsave r
        r debug reload
        r get x
    } {10}

    test {Perform a final SAVE to leave a clean DB on disk} {
        r debug flushall
        waitForBgsave r
        r save
    } {OK}
}
