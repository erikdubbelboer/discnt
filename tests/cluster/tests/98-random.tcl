source "../tests/includes/init-tests.tcl"    

test "Random for 10 minutes" {
    if {false} {
        for {set i 0} {$i < [expr {(10*60*1000)/10}]} {incr i} {
            after 10
            set n [expr {int(rand()*$::instances_count)}]
            set k [format "test:%d" [expr {int(rand()*100)}]]
            set v [expr {0.5 - rand()}]
            if {rand() < 0.1} {
                if {rand() < 0.1} {
                    D $n set $k 0
                } else {
                    D $n set $k $v
                }
            } else {
                D $n incrby $k $v
            }
        }
    }
}

