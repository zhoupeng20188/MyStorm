package com.zp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBlot extends BaseRichBolt {

        /**
         * tuple发射器
         */
        private OutputCollector outputCollector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        /**
         * 每次接受到一条数据，会执行这个方法
         *
         * @param tuple
         */
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] split = sentence.split(" ");
            for (String s : split) {
                outputCollector.emit(new Values(s));
            }

        }

        /**
         * 定义每个tuple的字段名
         *
         * @param outputFieldsDeclarer
         */
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }