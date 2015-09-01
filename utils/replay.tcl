#!/usr/bin/env tclsh8.5
# Copyright (C) 2015 Erik Dubbelboer
# Released under the BSD license like Discnt itself
#
# Reply output from MONITOR passed on stdin.

source "../tests/support/redis.tcl"

set ::host "127.0.0.1"
set ::port 5262
set ::verbose 0

proc main {} {
    set r [redis $::host $::port]

    while 1 {
        set l [gets stdin]
        if {[eof stdin]} {
            break
        }
        # MONITOR output always starts with an OK.
        if {$l == "OK"} {
            continue
        }
        
        set l [string map {"\"" ""} $l]

        set c [split $l]
        set c [lreplace $c 0 1]

        if {$::verbose} {
            puts $c
        }
        set v [$r {*}$c]
        if {$::verbose} {
            puts $v
        }
    }
}

proc help {} {
    puts "Usage: ./replay.tcl \[OPTIONS\]"
    puts "  -h <hostname>      Server hostname (default: 127.0.0.1)."
    puts "  -p <port>          Server port (default: 5262)."
    puts "  --help             Output this help and exit."
    puts ""
    puts "Examples:"
    puts "  discnt-cli monitor | ./replay.tcl -p 1234"
    exit 0
}

# Force the user to run the script from the 'utils' directory.
if {![file exists replay.tcl]} {
    puts "Please make sure to run replay.tcl while inside /utils."
    puts "Example: cd ~/discnt/utils; ./replay.tcl < ~/monitor.out"
    exit 1
}

for {set j 0} {$j < [llength $argv]} {incr j} {
    set opt [lindex $argv $j]
    set arg [lindex $argv [expr $j+1]]
    if {$opt eq {-h}} {
        if {$arg == ""} {
            help
        }
        set ::host $arg
        incr j
    } elseif {$opt eq {-p}} {
        set ::port $arg
        incr j
    } elseif {$opt eq {-v}} {
        set ::verbose 1
    } elseif {$opt eq {--help}} {
        help
    } else {
        puts "Wrong argument: $opt"
        exit 1
    }
}

main
