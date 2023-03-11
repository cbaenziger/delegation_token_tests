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
import java.nio.file.Files;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSTest {
    public static final Logger logger = LoggerFactory.getLogger(HDFSTest.class);

    /*
     * Combine two Hadoop Configuration objects to include the HDFS configuration from both
     */
    public static Configuration combineHDFSes(Configuration primary, Configuration secondary) {
	Configuration newConf = new Configuration(primary);
	String secondaryHDFS = secondary.get("dfs.nameservices");
	newConf.set("dfs.nameservices", primary.get("dfs.nameservices") + "," + secondaryHDFS);
	for (Map.Entry<String,String> hdfsConf : secondary.getValByRegex("dfs\\..*" + secondaryHDFS).entrySet()) {
	    logger.warn("Adding configuration  {}", hdfsConf.getKey());
            newConf.set(hdfsConf.getKey(), hdfsConf.getValue());
	}
	return(newConf);
    }

    public static void main(String[] args) {
	String dirName = args[0];
	logger.warn("Running for argument {}", dirName);
	if (!dirName.endsWith("/")) {
            throw new IllegalArgumentException("No trailing slash on directory name (" + dirName + ")");
	}
	List<String> confDirs = new ArrayList<>();
	confDirs.add(System.getenv("HADOOP_CONF_DIR"));
	if(System.getenv("HADOOP_CONF_DIR2") != null) {
	    confDirs.add(System.getenv("HADOOP_CONF_DIR2"));
	}

	List<Configuration> confs = new ArrayList<>();
	for (String confDir : confDirs) {
            Configuration conf = new Configuration();
            conf.addResource(new Path(confDir + "/core-site.xml"));
            conf.addResource(new Path(confDir + "/hdfs-site.xml"));
            // Control FS instances explicitly and do not cache them
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.setInt("dfs.client.write.exclude.nodes.cache.expiry.interval.millis", 1000);
            conf.setInt("dfs.client.server-defaults.validity.period.ms", 1000);
            conf.setInt("dfs.client.key.provider.cache.expiry", 1000);
            conf.setInt("dfs.client.cached.conn.retry", 1);
            conf.setInt("ipc.client.connect.max.retries", 1);
            conf.setInt("ipc.client.connect.retry.interval", 10);
            conf.setInt("dfs.client.retry.max.attempts", 0);
            conf.setInt("dfs.client.failover.sleep.max.millis", 500);
            conf.set("dfs.client.retry.policy.spec", "1000,1,2000,1");
            conf.setBoolean("dfs.client.retry.policy.enabled", true);
	    // update proxy provider to RequestHedging to find the NN faster
	    for (String proxyProvider : conf.getValByRegex("dfs\\.client\\.failover\\.proxy\\.proivder\\..*").keySet()) {
                conf.set(proxyProvider, "org.apache.hadoop.hdfs.server.namenode.ha.RequestHedgingProxyProvider");
	    }
            // Use delegation token auto-renewal
            conf.setBoolean("hadoop.delegation.token.login.autorenewal.enabled", true);
            conf.setInt("hadoop.delegation.token.min.seconds.before.relogin", 20);
            confs.add(conf);
	}
	Configuration conf = confs.get(0);
	if(confs.size() > 1) {
            for(int i = 1; i < confs.size(); i++) {
                conf = combineHDFSes(conf, confs.get(i));
	    }
	}
	conf.unset("fs.defaultFS");
	conf.unset("dfs.internal.nameservices");
        try {
            Files.deleteIfExists(java.nio.file.Paths.get("./output-site.xml"));
	} catch (IOException e) {
	    e.printStackTrace();
	    java.lang.System.exit(-1);
        }
        try(Writer writer = new FileWriter("./output-site.xml")) {
            conf.writeXml(null, writer);
	} catch (IOException e) {
	    e.printStackTrace();
	    java.lang.System.exit(-1);
        }

	String overwriteFileName = "overwrite_file";
	String appendFileName = "append_file";
	Path dirPath = null;

	UserGroupInformation.setConfiguration(conf);
	for (String fsName : conf.getTrimmedStringCollection("dfs.nameservices")) {
            URI fsURI = null;
	    try {
                fsURI = new URI("hdfs://" + fsName);
	    } catch (URISyntaxException e) {
		e.printStackTrace();
	        java.lang.System.exit(-1);
	    }
	    try {
	        UserGroupInformation.getLoginUser();
	        FileSystem fsLongLived = FileSystem.newInstance(fsURI, conf);

	        dirPath = new Path(dirName);
                logger.info("Starting HDFS Writing Test for directory {} on filesystem {}/{}", dirName, fsLongLived.getCanonicalServiceName(), fsLongLived.getUri());
                if (!fsLongLived.exists(dirPath)) {
                    logger.info("Making directory {}", dirName);
                    fsLongLived.mkdirs(dirPath);
	        }

                String[] files = {appendFileName, overwriteFileName};
                for (String f : files){
	            Path p = new Path(dirName + f);
                    if (fsLongLived.exists(p)) {
	                logger.info("Deleting file at {}", p);
	                fsLongLived.delete(p);
	            }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	        try {
                    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                    logger.warn("Failed to auth using {} authentication", ugi.getAuthenticationMethod());
                    logger.warn("ticket: {} keytab: {}", UserGroupInformation.isLoginTicketBased(), UserGroupInformation.isLoginKeytabBased());
	        } catch (IOException ioe) {
	            ioe.printStackTrace();
	        }
	        java.lang.System.exit(-1);
	    }
	}

	while (true) {
	    for (String fsName : conf.getTrimmedStringCollection("dfs.nameservices")) {
              URI fsURI = null;
	      try {
                  fsURI = new URI("hdfs://" + fsName);
              } catch (URISyntaxException e) {
                  e.printStackTrace();
                  java.lang.System.exit(-1);
              }
	      try {
	          FileSystem fs = FileSystem.newInstance(fsURI, conf);
	          DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	          LocalDateTime now = LocalDateTime.now();
	          Path overwriteFilePath = new Path(dirName + overwriteFileName);
	          logger.info("Writing overwriteFile at {}", overwriteFilePath);
                  FSDataOutputStream overwriteFile = fs.create(overwriteFilePath);
	          overwriteFile.writeUTF(dtf.format(now));
	          overwriteFile.flush();
	          overwriteFile.close();

	          Path appendFilePath = new Path(dirName + appendFileName);
	          logger.info("Appending appendFile at {}", appendFilePath);
	          if (!fs.exists(appendFilePath)) {
	              FSDataOutputStream appendFile = fs.create(appendFilePath);
	              appendFile.writeUTF("Initial write " + dtf.format(now) + "\n");
	              appendFile.flush();
	              appendFile.close();
	          }
                  FSDataOutputStream appendFile = fs.append(appendFilePath);
	          appendFile.writeUTF(dtf.format(now) + "\n");
	          appendFile.flush();
	          appendFile.close();

	          logger.info("Sleeping...");
	          // it seems like we successfully fail with expired tokens after 60+epsilon seconds
	          Thread.sleep(60001);
	      } catch (IOException|InterruptedException e) {
	          e.printStackTrace();
	          java.lang.System.exit(-1);
	      }
	    }
	}
    }
}
