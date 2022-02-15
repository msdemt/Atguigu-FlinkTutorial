package org.example.flink.wc;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

/**
 * @Description: 纳税人月度开票额统计
 * <p>
 * {"nsrsbh":"15000120561127953X","fjh":"0","je":"433773.9","se":"21688.7","kprq":"2021-10-11 11:44:56"}
 * {"nsrsbh":"110101MYJ2GPQQ4","fjh":"0","je":"433.1","se":"216.7","kprq":"2021-10-11 12:44:56"}
 * @Author: hekai
 * @Date: 2022-02-15 10:16
 */
public class UserMonthlyCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<User> userStream = dataStreamSource
                .map(data -> new Gson().fromJson(data, User.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                Date date = null;
                                try {
                                    date = sdf.parse(element.getKprq());
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                Long time = date.getTime();
                                return time;
                            }
                        }));

        userStream.keyBy(User::getNsrsbh)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(16))) //https://blog.csdn.net/maomaoqiukqq/article/details/104334993
                //.process(new UserProcess())  //全窗口函数，先把窗口所有数据收集起来，等到计算的时候遍历所有数据
                .process(new ReduceFunction)

                .print();

        env.execute();
    }

    private static class UserProcess extends ProcessWindowFunction<User, Object, String, TimeWindow> {

        private ValueState<BigDecimal> jeState;
        private ValueState<BigDecimal> seState;

        @Override
        public void process(String s, ProcessWindowFunction<User, Object, String, TimeWindow>.Context context, Iterable<User> elements, Collector<Object> out) throws Exception {
            //Integer count = IteratorUtils.toList(elements.iterator()).size();

            Iterator iterator = elements.iterator();
            while (iterator.hasNext()) {
                User user = (User) iterator.next();
                BigDecimal je = new BigDecimal(user.getJe());
                BigDecimal se = new BigDecimal(user.getSe());
                if (jeState.value() == null) {
                    jeState.update(je);
                    seState.update(se);
                } else {
                    jeState.update(jeState.value().add(je));
                    seState.update(seState.value().add(se));
                }

                String kprq = user.getKprq();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = sdf.parse(kprq);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                SimpleDateFormat sdfDay = new SimpleDateFormat("yyyy-MM-dd");
                String dateDay = sdfDay.format(date);

                out.collect("税号：" + user.getNsrsbh() + "，日期：" + dateDay + "，累计金额：" + jeState.value() + "，累计税额：" + seState.value());
            }
        }

        //@Override
        //public void processElement(User user, KeyedProcessFunction<String, User, String>.Context ctx, Collector<String> out) throws Exception {
        //    BigDecimal je = new BigDecimal(user.getJe());
        //    BigDecimal se = new BigDecimal(user.getSe());
        //    if(jeState.value() == null){
        //        jeState.update(je);
        //        seState.update(se);
        //    }else{
        //        jeState.update(jeState.value().add(je));
        //        seState.update(seState.value().add(se));
        //    }
        //    out.collect("税号：" + user.getNsrsbh() + "，合计金额：" + jeState.value() + "，合计税额：" + seState.value());
        //}


        @Override
        public void open(Configuration parameters) throws Exception {
            jeState = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("je-state", BigDecimal.class));
            seState = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("se-state", BigDecimal.class));
        }

        @Override
        public void close() throws Exception {
            jeState.clear();
            seState.clear();
        }
    }
}
