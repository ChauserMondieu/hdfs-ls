package com.didichuxing.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * static method providing hdfs singleton configuration conf
 */
public class LsConfiguration {

    private static Configuration conf;

    /**
     * get hdfs configuration - singleton
     *
     * @return
     */
    public static Configuration getConf() {
        if (conf == null) {
            synchronized (LsConfiguration.class) {
                if (conf == null) {
                    conf = new Configuration();
                    conf.addResource("core-site.xml");
                    conf.addResource("hdfs-site.xml");
                    conf.set("HADOOP_USER_NAME", "${HADOOP_USER_NAME}");
                    conf.set("HADOOP_USER_PASSWORD", "${HADOOP_USER_PASSWORD}");
                }
            }
        }
        return conf;
    }
}
