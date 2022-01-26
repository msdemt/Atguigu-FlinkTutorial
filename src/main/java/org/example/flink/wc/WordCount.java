package org.example.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description: 批处理wordcount
 * @Author: hekai
 * @Date: 2022-01-24 13:11
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        //String inputPath = "D:\\workspace\\IdeaProjects\\flink\\Atguigu-FlinkTutorial\\src\\main\\resources\\hello.txt";
        String inputPath = WordCount.class.getClassLoader().getResource("hello.txt").getPath();
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        //空格分词打散之后，对单词进行groupby分组，然后用sum进行聚合
        AggregateOperator<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1); //将第二个位置上的数据求和
        resultSet.print();
    }

    private static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按空格分词
            String[] words = s.split(" ");
            //遍历所有word，包成二元组输出
            for (String word:words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
