start_server {} {
    set i [r info]
    regexp {discnt_version:(.*?)\r\n} $i - version
    regexp {discnt_git_sha1:(.*?)\r\n} $i - sha1
    puts "Testing Discnt version $version ($sha1)"
}
