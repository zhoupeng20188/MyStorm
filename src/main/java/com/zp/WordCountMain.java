package com.zp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @Author zp
 * @create 2020/11/16 9:55
 */
public class WordCountMain {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("RandomSentenceSpout", new com.zp.RandomSentenceSpout(), 2);
        // 10表示executor的个数，20表示task的个数
        topologyBuilder.setBolt("SplitSentenceBlot", new SplitSentenceBlot(), 5)
                                .setNumTasks(10)
                                .shuffleGrouping("RandomSentenceSpout");
        // 设置fieldsGrouping，相同单词会进到同一个task中,保证单词统计的准确性
        topologyBuilder.setBolt("WordCountBolt", new WordCountBolt(), 10)
                                .setNumTasks(20)
                                .fieldsGrouping("SplitSentenceBlot", new Fields("word"));

        Config config = new Config();
        // 设置最大并行度
        config.setMaxTaskParallelism(3);
        // 本地运行
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("WordCount", config, topologyBuilder.createTopology());
        Utils.sleep(60000);
        localCluster.shutdown();
    }






}
