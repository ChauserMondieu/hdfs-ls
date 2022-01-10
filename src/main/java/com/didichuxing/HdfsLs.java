package com.didichuxing;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.conf.HdfsFactory;
import com.didichuxing.pojo.Data;
import com.didichuxing.utils.LogUtil;
import com.google.common.collect.Lists;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class HdfsLs {

    /**
     * args[0] indicator
     * args[1] logutil path
     * args[2] input file path
     * @param args params
     */
    public static void main(String[] args) {
        if(args.length != 3){
            System.err.println("wrong input, please try again!");
            return;
        }
        HdfsFactory hdfsFactory = new HdfsFactory(args[1]);
        HdfsLs hdfsLs = new HdfsLs();
        AlgoTemplate algo = hdfsFactory.getAlgo(args[0]);
        List<Data> list = hdfsLs.infos(args[2]);
        for (Data data: list) {
            algo.hdfsLs(data.getSrc(), data.getDest());
        }
        LogUtil.close();
    }


    /**
     * separate src & dst from sigle input file
     *
     * @param fileName
     * @return
     */
    public List<Data> infos(String fileName) {
        File file = new File(fileName);
        List<Data> lists = Lists.newArrayList();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String temp = null;
            Data data = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((temp = reader.readLine()) != null) {
                // 显示行号
                String[] temps = temp.split("\\s+");
                if (temps.length == 2) {
                    data = new Data(temps[0], temps[1]);
                    lists.add(data);
                } else {
                    System.err.println("read error line " + line + ": " + temp + ":" + temps.length);
                }
                line++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
        }
        return lists;
    }
}


