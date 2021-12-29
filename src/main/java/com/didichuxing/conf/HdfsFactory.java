package com.didichuxing.conf;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.algo.impl.algoAuth;
import com.didichuxing.algo.impl.algoDiff;
import com.didichuxing.algo.impl.algoFile;
import com.didichuxing.algo.impl.algoLeaf;
import com.didichuxing.algo.impl.algoNull;
import com.didichuxing.algo.impl.algoRoot;
import com.didichuxing.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hdfs-ls功能的工厂类
 */
public class HdfsFactory {

    static enum Indicator{
        // 用于权限校验
        AUTH_INDICATOR("auth",1),
        // 用于将根目录向下拆分一级
        DIFF_INDICATOR("diff",2),
        // 用于将根目录向下拆分一级并包含根目录
        ROOT_INDICATOR("root",3),
        // 用于将根目录拆分至叶子目录
        LEAF_INDICATOR("leaf",4),
        // 用于判断目录是否存在
        NULL_INDICATOR("null",5),
        FILE_INDICATOR("file",6);

        private final String name;
        private final Integer flag;

        Indicator(String name, Integer flag) {
            this.name = name;
            this.flag = flag;
        }
    }

    private final Logger logger = LoggerFactory.getLogger(HdfsFactory.class);
    private final String path;

    /**
     * factory should prepare log path for each algorithm
     * @param path
     */
    public HdfsFactory(String path) {
        this.path = path;
    }

    /**
     * return algorithm for each indicator choice
     * @param indicator
     * @return
     */
    public AlgoTemplate getAlgo(String indicator) {
        if (Indicator.AUTH_INDICATOR.name.equals(indicator)) {
            return new algoAuth(path);
        }else if(Indicator.DIFF_INDICATOR.name.equals(indicator)){
            return new algoDiff(path);
        }else if(Indicator.ROOT_INDICATOR.name.equals(indicator)){
            return new algoRoot(path);
        }else if(Indicator.LEAF_INDICATOR.name.equals(indicator)){
            return new algoLeaf(path);
        }else if(Indicator.NULL_INDICATOR.name.equals(indicator)){
            return new algoNull(path);
        }else if(Indicator.FILE_INDICATOR.name.equals(indicator)){
            return new algoFile(path);
        }else{
            System.err.println("wrong input!");
            return null;
        }
    }

    public AlgoTemplate getAlgo(int indicator) {
        if (Indicator.AUTH_INDICATOR.flag.equals(indicator)) {
            return new algoAuth(path);
        }else if(Indicator.DIFF_INDICATOR.flag.equals(indicator)){
            return new algoDiff(path);
        }else if(Indicator.ROOT_INDICATOR.flag.equals(indicator)){
            return new algoRoot(path);
        }else if(Indicator.LEAF_INDICATOR.flag.equals(indicator)){
            return new algoLeaf(path);
        }else if(Indicator.NULL_INDICATOR.flag.equals(indicator)){
            return new algoNull(path);
        }else if(Indicator.FILE_INDICATOR.flag.equals(indicator)){
            return new algoFile(path);
        }else{
            System.err.println("wrong input!");
            return null;
        }
    }

}
