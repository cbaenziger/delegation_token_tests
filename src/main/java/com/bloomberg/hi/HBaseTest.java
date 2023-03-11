/*
 ** Copyright 2023 Bloomberg Finance L.P.
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 */

package com.bloomberg.hi;

import java.io.*;
import java.util.*;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTest {

    public static void main(String[] args) {
	Logger logger = LoggerFactory.getLogger(HBaseTest.class);
	String table_name = args[0];
	logger.info("Starting HBase Writing Test for table {}", table_name);
	Configuration conf = HBaseConfiguration.create();
	String confDir = System.getenv("HBASE_CONF_DIR");
        conf.addResource(new Path(confDir + "/core-site.xml"));
        conf.addResource(new Path(confDir + "/hbase-site.xml"));
        conf.addResource(new Path(confDir + "/hdfs-site.xml"));
	// Configuration to enable delegation token refresh. This should be set in
	// core-site.xml or set in application if you do not control your XML
	conf.set("hadoop.delegation.token.min.seconds.before.relogin", "20");
	conf.setBoolean("hadoop.delegation.token.login.autorenewal.enabled", true);

	// For testing, make sure to expire RS connection cache to ensure a
	// new connection (using authentication is alwasy used)
	// DO NOT SET THIS SO LOW IN PRODUCTION
        conf.set("hbase.ipc.client.connection.minIdleTimeBeforeClose", "10000");

	// Ensure configuration is set as expected
	logger.warn("hadoop.delegation.token.min.seconds.before.relogin {}",
            conf.getTrimmed("hadoop.delegation.token.min.seconds.before.relogin"));
	logger.warn("hadoop.delegation.token.login.autorenewal.enabled {}",
            conf.getTrimmed("hadoop.delegation.token.login.autorenewal.enabled"));
	logger.warn("hbase.ipc.client.connection.minIdleTimeBeforeClose: {}",
            conf.getTrimmed("hbase.ipc.client.connection.minIdleTimeBeforeClose"));

	Table table = null;
	// Iniitalize Hadoop metrics subsystem to see the following via JMX:
	// Hadoop:name=UgiMetrics,service=MyApplication,UgiMetrics:
	// RenewalFailuresTotal - “Renewal failures since startup”
	// RenewalFailures - "Renewal failures since last successful renewal"
        DefaultMetricsSystem.initialize("HBaseTest");
        JvmMetrics.initSingleton("HBaseTest", "");
	try {
	    // UGI does not reload configuration information by default
	    // Unless Hadoop is built with delegation token renewal being enabled
	    // in core-default.xml this will need to be run to configure token
	    // refresh
            UserGroupInformation.setConfiguration(conf);

	    // Generic HBase read/write test below
	    Connection connection = ConnectionFactory.createConnection(conf);
	    table = connection.getTable(TableName.valueOf(table_name));
	} catch (IOException e) {
	    e.printStackTrace();
	    java.lang.System.exit(-1);
	}

	while (true) {
	    try {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
	        byte[] key = Bytes.toBytes("row-by-java-client");
	        byte[] val = Bytes.toBytes(dtf.format(now));
	    
	        Put p = new Put(key);
	        byte[] family = Bytes.toBytes("info");
	        byte[] column = Bytes.toBytes("column");
	        p.addColumn(family, column, val);
	        table.put(p);
	    
	        Get g = new Get(key);
	        Result r = table.get(g);
	        System.out.println("Get: " + r);
	    
	        Scan scan = new Scan();
	        ResultScanner scanner = table.getScanner(scan);
	        try {
	            for (Result sr: scanner)
	                System.out.println("Scan: " + sr);
	        } finally {
	            scanner.close();
	        }
	    
	        byte[] start = Bytes.toBytes("row3");
	        scan = new Scan(start);
	        scanner = table.getScanner(scan);
	        try {
	            for (Result sr: scanner)
	                System.out.println("Scan: " + sr);
	        } finally {
	            scanner.close();
	        }
	        Thread.sleep(5000);
	    } catch (NotServingRegionException e) {
	        e.printStackTrace();
	    } catch (IOException|InterruptedException e) {
	        e.printStackTrace();
	        java.lang.System.exit(-1);
	    }
	}
    }
}
