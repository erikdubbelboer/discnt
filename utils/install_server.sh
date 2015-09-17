#!/bin/sh

# Copyright 2015 Erik Dubbelboer <erik at dubbelboer dot com>. All rights reserved.
# Copyright 2011 Dvir Volk <dvirsk at gmail dot com>. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL Dvir Volk OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
################################################################################
#
# Interactive service installer for discnt server
# this generates a discnt config file and an /etc/init.d script, and installs them
# this scripts should be run as root

die () {
	echo "ERROR: $1. Aborting!"
	exit 1
}


#Absolute path to this script
SCRIPT=$(readlink -f $0)
#Absolute path this script is in
SCRIPTPATH=$(dirname $SCRIPT)

#Initial defaults
_DISCNT_USER=root
_DISCNT_PORT=5262

echo "Welcome to the discnt service installer"
echo "This script will help you easily set up a running discnt server"
echo

#check for root user
if [ "$(id -u)" -ne 0 ] ; then
	echo "You must run this script as root. Sorry!"
	exit 1
fi

#Read the discnt user
read  -p "Please select the user for this instance: [$_DISCNT_USER] " DISCNT_USER
if [ -z "$DISCNT_USER" ] ; then
	echo "Selecting default: $_DISCNT_USER"
	DISCNT_USER=$_DISCNT_USER
fi

#Read the discnt port
read  -p "Please select the discnt port for this instance: [$_DISCNT_PORT] " DISCNT_PORT
if ! echo $DISCNT_PORT | egrep -q '^[0-9]+$' ; then
	echo "Selecting default: $_DISCNT_PORT"
	DISCNT_PORT=$_DISCNT_PORT
fi

#read the discnt config file
_DISCNT_CONFIG_FILE="/etc/discnt/$DISCNT_PORT.conf"
read -p "Please select the discnt config file name [$_DISCNT_CONFIG_FILE] " DISCNT_CONFIG_FILE
if [ -z "$DISCNT_CONFIG_FILE" ] ; then
	DISCNT_CONFIG_FILE=$_DISCNT_CONFIG_FILE
	echo "Selected default - $DISCNT_CONFIG_FILE"
fi

#read the discnt log file path
_DISCNT_LOG_FILE="/var/log/discnt_$DISCNT_PORT.log"
read -p "Please select the discnt log file name [$_DISCNT_LOG_FILE] " DISCNT_LOG_FILE
if [ -z "$DISCNT_LOG_FILE" ] ; then
	DISCNT_LOG_FILE=$_DISCNT_LOG_FILE
	echo "Selected default - $DISCNT_LOG_FILE"
fi


#get the discnt data directory
_DISCNT_DATA_DIR="/var/lib/discnt/$DISCNT_PORT"
read -p "Please select the data directory for this instance [$_DISCNT_DATA_DIR] " DISCNT_DATA_DIR
if [ -z "$DISCNT_DATA_DIR" ] ; then
	DISCNT_DATA_DIR=$_DISCNT_DATA_DIR
	echo "Selected default - $DISCNT_DATA_DIR"
fi

#get the discnt executable path
_DISCNT_EXECUTABLE=`command -v discnt-server`
read -p "Please select the discnt executable path [$_DISCNT_EXECUTABLE] " DISCNT_EXECUTABLE
if [ ! -x "$DISCNT_EXECUTABLE" ] ; then
	DISCNT_EXECUTABLE=$_DISCNT_EXECUTABLE

	if [ ! -x "$DISCNT_EXECUTABLE" ] ; then
		echo "Mmmmm...  it seems like you don't have a discnt executable. Did you run make install yet?"
		exit 1
	fi
fi

#check the default for discnt cli
CLI_EXEC=`command -v discnt-cli`
if [ -z "$CLI_EXEC" ] ; then
	CLI_EXEC=`dirname $DISCNT_EXECUTABLE`"/discnt-cli"
fi

echo "Selected config:"

echo "User           : $DISCNT_USER"
echo "Port           : $DISCNT_PORT"
echo "Config file    : $DISCNT_CONFIG_FILE"
echo "Log file       : $DISCNT_LOG_FILE"
echo "Data dir       : $DISCNT_DATA_DIR"
echo "Executable     : $DISCNT_EXECUTABLE"
echo "Cli Executable : $CLI_EXEC"

read -p "Is this ok? Then press ENTER to go on or Ctrl-C to abort." _UNUSED_

mkdir -p `dirname "$DISCNT_CONFIG_FILE"` || die "Could not create discnt config directory"
mkdir -p `dirname "$DISCNT_LOG_FILE"` || die "Could not create discnt log dir"
mkdir -p "$DISCNT_DATA_DIR" || die "Could not create discnt data directory"

chown $DISCNT_USER "$DISCNT_DATA_DIR" || die "Could not chown discnt data directory"
touch "$DISCNT_LOG_FILE" || die "Could not create discnt log file"
chown $DISCNT_USER "$DISCNT_LOG_FILE" || die "Could not chown discnt log file"

#render the templates
TMP_FILE="/tmp/${DISCNT_PORT}.conf"
DEFAULT_CONFIG="${SCRIPTPATH}/../discnt.conf"
INIT_TPL_FILE="${SCRIPTPATH}/discnt_init_script.tpl"
INIT_SCRIPT_DEST="/etc/init.d/discnt_${DISCNT_PORT}"
PIDFILE="/var/run/discnt_${DISCNT_PORT}.pid"

if [ ! -f "$DEFAULT_CONFIG" ]; then
	echo "Mmmmm... the default config is missing. Did you switch to the utils directory?"
	exit 1
fi

#Generate config file from the default config file as template
#changing only the stuff we're controlling from this script
echo "## Generated by install_server.sh ##" > $TMP_FILE

read -r SED_EXPR <<-EOF
s#^port [0-9]{4}\$#port ${DISCNT_PORT}#; \
s#^logfile .+\$#logfile ${DISCNT_LOG_FILE}#; \
s#^dir .+\$#dir ${DISCNT_DATA_DIR}#; \
s#^pidfile .+\$#pidfile ${PIDFILE}#; \
s#^daemonize no\$#daemonize yes#;
EOF
sed -r "$SED_EXPR" $DEFAULT_CONFIG  >> $TMP_FILE

#cat $TPL_FILE | while read line; do eval "echo \"$line\"" >> $TMP_FILE; done
cp $TMP_FILE $DISCNT_CONFIG_FILE || die "Could not write discnt config file $DISCNT_CONFIG_FILE"

#Generate sample script from template file
rm -f $TMP_FILE

#we hard code the configs here to avoid issues with templates containing env vars
#kinda lame but works!
DISCNT_INIT_HEADER=\
"#!/bin/sh\n
#Configurations injected by install_server below....\n\n
EXEC=$DISCNT_EXECUTABLE\n
CLIEXEC=$CLI_EXEC\n
PIDFILE=\"$PIDFILE\"\n
CONF=\"$DISCNT_CONFIG_FILE\"\n
DISCNTPORT=\"$DISCNT_PORT\"\n
DISCNTUSER=\"$DISCNT_USER\"\n\n
###############\n\n"

DISCNT_CHKCONFIG_INFO=\
"# REDHAT chkconfig header\n\n
# chkconfig: - 58 74\n
# description: discnt_${DISCNT_PORT} is the discnt daemon.\n
### BEGIN INIT INFO\n
# Provides: discnt_6379\n
# Required-Start: \$network \$local_fs \$remote_fs\n
# Required-Stop: \$network \$local_fs \$remote_fs\n
# Default-Start: 2 3 4 5\n
# Default-Stop: 0 1 6\n
# Should-Start: \$syslog \$named\n
# Should-Stop: \$syslog \$named\n
# Short-Description: start and stop discnt_${DISCNT_PORT}\n
# Description: Discnt daemon\n
### END INIT INFO\n\n"

if command -v chkconfig >/dev/null; then
	#if we're a box with chkconfig on it we want to include info for chkconfig
	echo "$DISCNT_INIT_HEADER" "$DISCNT_CHKCONFIG_INFO" > $TMP_FILE && cat $INIT_TPL_FILE >> $TMP_FILE || die "Could not write init script to $TMP_FILE"
else
	#combine the header and the template (which is actually a static footer)
	echo "$DISCNT_INIT_HEADER" > $TMP_FILE && cat $INIT_TPL_FILE >> $TMP_FILE || die "Could not write init script to $TMP_FILE"
fi

###
# Generate sample script from template file
# - No need to check which system we are on. The init info are comments and
#   do not interfere with update_rc.d systems. Additionally:
#     Ubuntu/debian by default does not come with chkconfig, but does issue a
#     warning if init info is not available.

cat > ${TMP_FILE} <<EOT
#!/bin/sh
#Configurations injected by install_server below....

EXEC=$DISCNT_EXECUTABLE
CLIEXEC=$CLI_EXEC
PIDFILE=$PIDFILE
CONF="$DISCNT_CONFIG_FILE"
DISCNTPORT="$DISCNT_PORT"
DISCNTUSER="$DISCNT_USER"

###############
# SysV Init Information
# chkconfig: - 58 74
# description: discnt_${DISCNT_PORT} is the discnt daemon.
### BEGIN INIT INFO
# Provides: discnt_${DISCNT_PORT}
# Required-Start: \$network \$local_fs \$remote_fs
# Required-Stop: \$network \$local_fs \$remote_fs
# Default-Start: 2 3 4 5
# Default-Stop: 0 1 6
# Should-Start: \$syslog \$named
# Should-Stop: \$syslog \$named
# Short-Description: start and stop discnt_${DISCNT_PORT}
# Description: Discnt daemon
### END INIT INFO

EOT
cat ${INIT_TPL_FILE} >> ${TMP_FILE}

#copy to /etc/init.d
cp $TMP_FILE $INIT_SCRIPT_DEST && \
	chmod +x $INIT_SCRIPT_DEST || die "Could not copy discnt init script to  $INIT_SCRIPT_DEST"
echo "Copied $TMP_FILE => $INIT_SCRIPT_DEST"

#Install the service
echo "Installing service..."
if command -v chkconfig >/dev/null 2>&1; then
	# we're chkconfig, so lets add to chkconfig and put in runlevel 345
	chkconfig --add discnt_${DISCNT_PORT} && echo "Successfully added to chkconfig!"
	chkconfig --level 345 discnt_${DISCNT_PORT} on && echo "Successfully added to runlevels 345!"
elif command -v update-rc.d >/dev/null 2>&1; then
	#if we're not a chkconfig box assume we're able to use update-rc.d
	update-rc.d discnt_${DISCNT_PORT} defaults && echo "Success!"
else
	echo "No supported init tool found."
fi

/etc/init.d/discnt_$DISCNT_PORT start || die "Failed starting service..."

#tada
echo "Installation successful!"
exit 0
