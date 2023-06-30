# SPDX-License-Identifier: GPL-2
# From https://github.com/TPC-Council/HammerDB/tree/master/scripts/tcl/maria/tproch

# BUILD

puts "SETTING CONFIGURATION"
dbset db maria
dbset bm TPC-H

diset connection maria_host $::env(MARIADB_HOST)
diset connection maria_port 3306

diset tpch maria_scale_fact 1
diset tpch maria_num_tpch_threads [ numberOfCPUs ]
diset tpch maria_tpch_user $::env(MARIADB_USER)
diset tpch maria_tpch_pass $::env(MARIADB_PASSWORD)
diset tpch maria_tpch_dbase $::env(MARIADB_DATABASE)
diset tpch maria_tpch_storage_engine innodb
puts "SCHEMA BUILD STARTED"
#buildschema
puts "SCHEMA BUILD COMPLETED"

# 
# TODO
# file based healthcheck trigger
# 
set tmpdir $::env(LOGS)

# RUN

loadscript
puts "TEST STARTED"
vuset vu 1
vucreate
set jobid [ vurun ]
vudestroy
puts "TEST COMPLETE"
set of [ open $tmpdir/maria_tproch w ]
puts $of $jobid
close $of

# DROP

puts " DROP SCHEMA STARTED"
#deleteschema
puts "DROP SCHEMA COMPLETED"
