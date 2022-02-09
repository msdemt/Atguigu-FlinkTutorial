package org.example.flink.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Description: java类型擦除，flink使用lambda表达式丢失类型信息
 * https://blog.csdn.net/qq_37555071/article/details/122161134
 *
 * @Author: hekai
 * @Date: 2022-02-09 10:32
 */
public class LambdaLostTypeTest {

    /**
     *
     * java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.String
     *
     * 	at org.example.flink.test.LambdaLostTypeTest.test1(LambdaLostTypeTest.java:22)
     */
    @Test
    public void test1(){
        List list = new ArrayList();
        list.add("abc");
        list.add(123);

        for (int i = 0; i < list.size(); i++) {
            String item = (String) list.get(i);
            System.out.println(item);
        }

        //使用泛型避免运行时出现类型错误
        ArrayList<String> arrayList = new ArrayList<>();
        //arrayList.add(123); //编译时就会提出此处有错误
    }


    /**
     * java在编译时会采取去泛型化的措施，即java中的泛型，只在编译时有效，在运行时会将泛型的相关信息擦除，
     * 编译器只会在对象进入JVM和离开JVM的边界处添加类型检查和转换的方法，泛型的信息不会进入到运行阶段，
     * 这就是所谓的泛型擦除。
     *
     * 泛型擦除有两种方式：
     * 方式一：Code Sharing，对同一原始类型下的泛型类型只生成同一份目标代码
     * 方式二：Code Specialization，对每一个泛型类型都生成不同的目标代码
     * java使用的是第一种方式，c++和C#使用的是第二种方式
     *
     * 所以test2中实际上都是 Class<? extends ArrayList> 的比较，所以输出的是true
     *
     */
    @Test
    public void test2(){
        ArrayList<String> stringArrayList = new ArrayList<>();
        ArrayList<Integer> integerArrayList = new ArrayList<>();

        Class<? extends ArrayList> stringArrayListClass = stringArrayList.getClass();
        Class<? extends ArrayList> integerArrayListClass = integerArrayList.getClass();

        System.out.println(stringArrayListClass == integerArrayListClass); //true
    }


    /**
     * 类型擦除对flink的影响
     */
    @Test
    public void test3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.fromCollection(Arrays.asList("hello", "world", "flink", "world", "hello"));

        //SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream1 = dataStream.map(word -> new Tuple2<>(word, 1));
        /**
         * org.apache.flink.api.common.functions.InvalidTypesException:
         * The return type of function 'test3(LambdaLostTypeTest.java:78)' could not be determined automatically,
         * due to type erasure. You can give type information hints by using the returns(...) method
         * on the result of the transformation call, or by letting your function implement
         * the 'ResultTypeQueryable' interface.
         */


        SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream2 = dataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        }); //可以执行成功

        //将上面的方法使用idea快捷键生成lambda表达式后，执行失败，错误同上
        //SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream1 = dataStream.map(
        //        (MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, 1)
        //);

        /**
         * 为什么使用lambda表达式，jvm就无法检测出Tuple2中的参数类型，而匿名类就可以呢？
         * Tuple2<T0, T1>中是有两个泛型的，使用匿名内部类时，会被真正编译为class文件。
         */

        //使用flink提供的类型暗示
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream3 = dataStream.map(word -> new Tuple2<>(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        mapedStream2.print();
        mapedStream3.print();
        env.execute();


    }
}
