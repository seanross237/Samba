from lxml import html
import time
import datetime
import csv
import sys
import psycopg2
import os
import subprocess


cxn = psycopg2.connect(service='ziggydb')
csr = cxn.cursor()

schema = sys.argv[1]



#get max min of _tads and assign it to start_date and end_date
max_min_ts_q = 'select min(ts), max(ts) from '+schema+'._tads'
csr.execute(max_min_ts_q)
results = csr.fetchall()

start_date = str(results[0][0])

end_date = str(results[0][1])

#COPY FILE FROM SCHEMA._TADS TO ~/DESKTOP/SCHEMA_TADS.CSV


copy_q = "copy (select sdid, ts, tad_id from " +schema+ "._tads) to STDOUT csv"
copy_fd = open(os.path.expanduser("~/desktop/"+schema+"_tads.csv"), "w")
print copy_q
print copy_fd
csr.copy_expert(copy_q, copy_fd)
copy_fd.flush()
copy_fd.close()

print 'downloaded _tads'
#UPLOAD FROM COMPUTER TO CHDBS

for chdb in ['chdb{}'.format(n) for n in range(11, 19)]:
	cxn = psycopg2.connect(service=chdb)
	chdb_csr = cxn.cursor()
	chdb_csr.execute("create unlogged table {}_tads_for_before_after (sdid bytea, ts timestamp, tad_id integer);".format(schema))
	chdb_csr.copy_expert("copy {}_tads_for_before_after from STDIN csv".format(schema),
		open(os.path.expanduser("~/desktop/"+schema+"_tads.csv")))
	cxn.commit()

print 'uploaded {}_tads_for_before_after'.format(schema)

#Run before and after

subprocess.call(["sh", "/Users/seanross/Desktop/before_after.sh", schema, start_date, end_date])


cxn = psycopg2.connect(service='ziggydb')
csr = cxn.cursor()

make_new_tads_q = 'create table '+schema+'._tads_with_cs as select * from '+schema+'._tads left join '+schema+'.'+schema+'_tad_id_cs using(tad_id);'
csr.execute(make_new_tads_q)
csn.commit()
