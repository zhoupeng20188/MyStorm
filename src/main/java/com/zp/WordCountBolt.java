package com.zp;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

        /**
         * tuple发射器
         */
        private OutputCollector outputCollector;

        private Map<String, Integer> map = new HashMap<String, Integer>();

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        /**
         * 每次接受到一条数据，会执行这个方法
         *
         * @param tuple
         */
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            Integer cnt = map.get(word);
            if (cnt == null) {
                cnt = 0;
            }
            cnt++;
            map.put(word, cnt);
            System.out.println("单词【"+ word + "】出现的次数是" + cnt);
            outputCollector.emit(new Values(word, cnt));

        }

        /**
         * 定义每个tuple的字段名
         *
         * @param outputFieldsDeclarer
         */
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "cnt"));
        }
    }