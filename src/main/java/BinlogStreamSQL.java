import bean.BinLogBean;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BinLogUtil;

import java.util.Properties;

/**
 * 在Canal BinLog上执行SQL操作
 * 使用 Flink StreamSQL 在 MySQL BinLog 上写 join sql 计算实时指标
 * @author lwj
 * @date 2018/12/6
 * @finish 2018/12/15
 */
/**
 * 如果程序挂掉了或者在savepoint后 想用之前state继续执行
 * 或者说程序挂掉了或者换版后想接着之前的程序继续执行
 * 那么在启动任务时需要添加参数： -s hdfs://checkpoint/before_job_id/chk-version
 * 比如：
 *  bin/flink run -s hdfs://ido001:8020/user/lwj/flink/checkpoint/d7450e5421422df163abbfd7a90729e8/chk-53 -c BinlogStreamSQL flink-1.0.0.jar
 */
public class BinlogStreamSQL {
    private static Logger log = LoggerFactory.getLogger(BinlogStreamSQL.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://ido001:8020/user/lwj/flink/checkpoint"));

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        //设置空闲state的保留时间
        /**
         * 对这个参数的个人理解：
         * minTime：如果该key在minTime时间内如果都没有变动的话就清除
         * maxTime：不管是否该在变动，该key在state中最多保存maxTime的时间
         * 以上是个人理解，官网对该方法的两个参数有模糊的解释，但是没有进行详细的解释，如有不对麻烦指点
         */
        tEnv.queryConfig().withIdleStateRetentionTime(Time.days(10), Time.days(30));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ido001.gzcb.com:9092,ido002.gzcb.com:9092,ido003.gzcb.com:9092");
        properties.setProperty("group.id", "flink");
        String topic = "bps-16-r3p3";

        DataStreamSource<String> topic1 = env.addSource(new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Tuple3<String, String, String>> kafkaSource1 = topic1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    BinLogBean binLogBean = JSONObject.parseObject(value, BinLogBean.class);
                    if ("app_case".equals(binLogBean.getTableName())){
                        return true;
                    }else {
                        return false;
                    }
                }catch (Exception e){
                    log.error("JSON转换失败，str={}", value, e);
                    return false;
                }
            }
        }).map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                BinLogBean binLogBean = JSONObject.parseObject(s, BinLogBean.class);
                String case_id = BinLogUtil.getValueByField(binLogBean, "case_id");
                String close_time = BinLogUtil.getValueByField(binLogBean, "close_time");
                String approve_result = BinLogUtil.getValueByField(binLogBean, "approve_result");
                return new Tuple3<String, String, String>(case_id, close_time, approve_result);
            }
        });
        tEnv.registerDataStream("app_case", kafkaSource1, "case_id, close_time, approve_result");

        DataStreamSource<String> topic2 = env.addSource(new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Tuple2<String, String>> kafkaSource2 = topic2.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    BinLogBean binLogBean = JSONObject.parseObject(value, BinLogBean.class);
                    if ("cm_customer".equals(binLogBean.getTableName())){
                        return true;
                    }else {
                        return false;
                    }
                }catch (Exception e){
                    log.error("JSON转换失败，str={}", value, e);
                    return false;
                }
            }
        }).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                BinLogBean binLogBean = JSONObject.parseObject(s, BinLogBean.class);
                String case_id = BinLogUtil.getValueByField(binLogBean, "case_id");
                String idtfno = BinLogUtil.getValueByField(binLogBean, "idtfno");
                return new Tuple2<String, String>(case_id, idtfno);
            }
        });
        tEnv.registerDataStream("cm_customer", kafkaSource2, "case_id, idtfno");

        Table result = tEnv.sqlQuery("select a.*,b.idtfno " +
                "from app_case a left join cm_customer b on a.case_id = b.case_id " +
                "where a.close_time not in('')");

        tEnv.toRetractStream(result, Row.class).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                return booleanRowTuple2.f0;
            }
        }).print();

        env.execute();
    }
}

