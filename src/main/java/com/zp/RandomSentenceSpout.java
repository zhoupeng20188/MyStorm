package com.zp;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Random random;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    /**
     * task会无限循环调用nextTuple()方法，
     * 可以不断发射数据，形成一个流
     */
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[]{"i am yours", "now is show time", "i want you", "i like you", "talk is cheap, show me the code"};
        //随机拿到一个句子
        String sentence = sentences[random.nextInt(sentences.length)];
        // 发射
        // Values构建一个tuple
        System.out.println("发射句子："+sentence);
        spoutOutputCollector.emit(new Values(sentence));
    }

    /**
     * 定义发射出去的tuple的字段的名称
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}