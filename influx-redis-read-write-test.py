#!/usr/bin/python

import argparse
import redis
import time
import sys
import influxdb
import copy
parser = argparse.ArgumentParser()
parser.add_argument("--redis_write", help="Write to redis",
                    action="store_true")
parser.add_argument("--redis_read", help="Read from redis",
                    action="store_true")
parser.add_argument("--redis_flush", help="Flush from redis",
                    action="store_true")
parser.add_argument("--redis_memory", help="Show Redis Memory",
                    action="store_true")
parser.add_argument("--influx_write", help="Write to influx",
                    action="store_true")
parser.add_argument("--influx_batch_write", help="Batch Write to influx",
                    action="store_true")
parser.add_argument("--influx_batch_line_write", help="Batch Line Write to influx",
                    action="store_true")
parser.add_argument("--influx_read", help="Read from influx",
                    action="store_true")
parser.add_argument("--influx_flush", help="Flush from influx",
                    action="store_true")

parser.add_argument("num_keys", help="keys/measurements to write/read", type=int)
parser.add_argument("num_values", help="values/points to write", type=int)
args = parser.parse_args()


if args.redis_read or args.redis_write or args.redis_flush or args.redis_memory:
    redis_conn = redis.client.StrictRedis()

if args.redis_flush:
    print("Doing redis flushall")
    redis_conn.flushall()

if args.redis_write:
    print("Doing redis write with %d keys and %d values" %(args.num_keys, args.num_values))
    start = time.time() 
    for j in range(0, args.num_keys):
        key = "nqv1#:#Temp#:#vx-1#:#temp%d#:#board sensor near cpu" % j
        for i in range(0, args.num_values):
            value = "{\"s_prev_state\":\"ok\",\"s_crit\":\"85\",\"s_lcrit\":\"-273\",\"s_input\":\"%s.000000\",\"s_msg\":\"\",\"s_min\":\"5\",\"active\":\"0\",\"timestamp\":\"1511826145.604597\",\"s_max\":\"80\",\"s_state\":\"ok\"}" % i
	    redis_conn.zadd(key, start+i, value)
    end = time.time()
    print("Time Taken: %f" % (end-start))

if args.redis_read:
    print("Doing redis read with %d keys" % args.num_keys)
    start = time.time() 
    for j in range(0, args.num_keys):
        key = "nqv1#:#Temp#:#vx-1#:#temp%d#:#board sensor near cpu" % j
        val = redis_conn.zrangebyscore(key, 0, 100000000000000000)
    end = time.time()
    print("Time Taken: %f" % (end-start))

if args.redis_memory:
    print("Reading memory usage")
    dump = redis_conn.info("memory")
    allowed_fields = ['used_memory_human', 'used_memory_rss',
                      'used_memory_peak_human']
    for header, value in dump.items():
        if header in allowed_fields:
            if header == 'used_memory_rss':
                value = str(float(value)/(1024*1024)) + 'M'
            print('%s: %s' % (header, value))


influx_db_name = 'netq'
influx_db_found = False
if args.influx_read or args.influx_write or args.influx_flush:
    influx_conn = influxdb.client.InfluxDBClient.from_dsn(dsn='influxdb://127.0.0.1')
    db_list = influx_conn.get_list_database()
    for db in db_list:
        for _, name in db.items():
            if name == influx_db_name:
                influx_db_found = True
    if args.influx_flush:
        print("Doing influx drop database")
        if influx_db_found:        
            influx_conn.query('DROP DATABASE %s' % influx_db_name)
            influx_db_found = False
    if not influx_db_found:
        print('Creating database %s' % influx_db_name)
        influx_conn.create_database(influx_db_name)
    influx_conn.switch_database(influx_db_name)    

if args.influx_write:    
    print("Doing influx write with %d measurements and %d values" %(args.num_keys, args.num_values))
    inf_dict = {}
    inf_dict['fields'] = {"min":5, "max":80, "critical":85,}
    inf_dict['tags'] = {"state":"ok", "message": "All good"}
    start = time.time() 
    for j in range(0, args.num_keys):
        inf_dict['measurement'] = "nqv1_Temp_vx-1_temp%d_boardsensornearcpu" % j
        for i in range(0, args.num_values):
            inf_dict['fields']['temp'] = i
            influx_conn.write_points(points=[inf_dict, ])
    end = time.time()
    print("Time Taken: %f" % (end-start))

if args.influx_batch_write:    
    print("Doing influx batch write with %d measurements and %d values" %(args.num_keys, args.num_values))
    inf_dict = {}
    inf_dict['fields'] = {"min":5, "max":80, "critical":85,}
    inf_dict['tags'] = {"state":"ok", "message": "Allgood"}
    inf_dict_list = []
    start = time.time() 
    for j in range(0, args.num_keys):
        inf_dict['measurement'] = "nqv1_Temp_vx-1_temp%d_boardsensornearcpu" % j
        for i in range(0, args.num_values):
    	    new_inf_dict = copy.deepcopy(inf_dict)
            new_inf_dict['fields']['temp'] = i
	    new_inf_dict['time'] = int((start + i) * 1000000000)
            inf_dict_list.append(new_inf_dict)
    print('Writing a batch of %d points' % len(inf_dict_list))
    influx_conn.write_points(points=inf_dict_list, batch_size=args.num_values)
    end = time.time()
    print("Time Taken: %f" % (end-start))

if args.influx_batch_line_write:
    print("Doing influx batch line write with %d measurements and %d values" %(args.num_keys, args.num_values))
    inf_dict = {}
    inf_dict['fields'] = {"min":5, "max":80, "critical":85,}
    inf_dict['tags'] = {"state":"ok", "message": "Allgood"}
    inf_dict_line_list = []
    start = time.time()
    for j in range(0, args.num_keys):
        inf_dict['measurement'] = "nqv1_Temp_vx-1_temp%d_boardsensornearcpu" % j
        for i in range(0, args.num_values):
            new_inf_dict = copy.deepcopy(inf_dict)
            new_inf_dict['fields']['temp'] = i
            new_inf_dict['time'] = int((start + i) * 1000000000)
            inf_str = inf_dict + ','.join('%s=%s' %(k, v) for k,v in new_inf_dict['tags'].items())
            inf_str += ' ' + ','.join('%s=%s' %(k, v) for k,v in new_inf_dict['fields'].items())
            inf_str += ' ' + new_inf_dict['time']
            print(inf_str)
            inf_dict_line_list.append(inf_str)

    print('Writing a batch of %d line points' % len(inf_dict_line_list))
    influx_conn.write_points(points=inf_dict_list, batch_size=args.num_values)
    end = time.time()
    print("Time Taken: %f" % (end-start))


if args.influx_read:
    print("Doing influx read with %d measurements" % args.num_keys)
    start = time.time() 
    for j in range(0, args.num_keys):
        inf_key = "nqv1_Temp_vx-1_temp%d_boardsensornearcpu" % j
	influx_conn.query('SELECT * from \"%s\"' % inf_key).get_points()
    end = time.time()
    print("Time Taken: %f" % (end-start))














