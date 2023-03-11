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
import java.lang.InterruptedException;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to move a region between region servers of an HBase cluster, moving the region
 * every configurable seconds. Used for testing authentication or other behaviors
 * where a region being moved every few seconds can be useful
 */
public class HBaseRegionMoverTest {

    public static void main(String[] args) {
	Logger logger = LoggerFactory.getLogger(HBaseRegionMoverTest.class);
	logger.warn("Starting HBase Region Mover Test for table {} every {} seconds", args[0], args[1]);
        String table_name = args[0];
        int sleepSec = Integer.parseInt(args[1]);
	Configuration conf = HBaseConfiguration.create();
	String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
	String hbaseConfDir = System.getenv("HBASE_CONF_DIR");
        conf.addResource(new Path(hbaseConfDir + "/hbase-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
	Admin admin = null;
	Connection connection = null;

	String[] secureConfgs = {"hadoop.security.authentication",
	"hbase.security.authentication",
	"hadoop.rpc.protection",
	"hbase.rpc.protection",
	"hbase.master.kerberos.principal"
	};
	for (String cfg: secureConfgs) {
            logger.warn("Config {} {}", cfg, conf.getTrimmed(cfg));
	}

	try {
            UserGroupInformation.setConfiguration(conf);
           // AuthUtil.getAuthChore(conf);
	    connection = ConnectionFactory.createConnection(conf);
	    admin = connection.getAdmin();
	} catch (IOException e) {
	    e.printStackTrace();
	    java.lang.System.exit(-1);
	}

	if (!UserGroupInformation.isSecurityEnabled()) {
	    logger.error("Security is not enabled; exiting.");
	    java.lang.System.exit(-1);
	}

        HRegionLocation regLoc = null;
	String nextRSName = null;
	ServerName nextRS = null;
	Set<ServerName> servers = null;
	Object[] viableServers = null;
	Random rand = new Random();

	while (true) {
	    try {
            	regLoc = connection.getRegionLocator(TableName.valueOf(table_name)).
			getAllRegionLocations().get(0);
            	byte[] encRegion = regLoc.getRegion().getEncodedNameAsBytes();
                final ServerName curRS = regLoc.getServerName();
		logger.warn("Current RS {}", curRS.getServerName());
		servers = admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS)).
			getLiveServerMetrics().keySet();
		viableServers = servers.stream().filter(s -> !s.equals(curRS)).collect(Collectors.toSet()).toArray();
		nextRS = (ServerName)viableServers[rand.nextInt(viableServers.length)];
		nextRSName = nextRS.getServerName();
		logger.warn("Next RS {}", nextRSName);

		admin.move(encRegion, Bytes.toBytes(nextRSName));

	        Thread.sleep(sleepSec * 1000);
	    } catch (NotServingRegionException e) {
	        e.printStackTrace();
	    } catch (IOException|InterruptedException e) {
	        e.printStackTrace();
	        java.lang.System.exit(-1);
	    }
	}
    }
}
