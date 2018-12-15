import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author lwj
 * @date 2018/12/6
 */
public class StreamSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<User> users = env.fromCollection(Arrays.asList(
                new User(1, "zhangsan", 1),
                new User(2, "zhangsan", 2),
                new User(3, "zhangsan", 3)
        ));
        DataStreamSource<Part> parts = env.fromCollection(Arrays.asList(
                new Part(1, "系统"),
                new Part(2, "运营"),
                new Part(3, "分期"),
                new Part(4, "大数据")
        ));

        tEnv.registerDataStream("users", users, "user_id,name,part_id");
        tEnv.registerDataStream("parts", parts, "part_id,part_name");

        Table result = tEnv.sqlQuery("select a.*,b.part_name from users a inner join parts b on a.part_id = b.part_id");

        tEnv.toRetractStream(result, User.class).print();

        env.execute();
    }



    public static class Part{
        private int part_id;
        private String part_name;

        public Part() {

        }

        public Part(int part_id, String part_name) {
            this.part_id = part_id;
            this.part_name = part_name;
        }

        public int getPart_id() {
            return part_id;
        }

        public void setPart_id(int part_id) {
            this.part_id = part_id;
        }

        public String getPart_name() {
            return part_name;
        }

        public void setPart_name(String part_name) {
            this.part_name = part_name;
        }
    }

    public static class User{
        private int user_id;
        private String name;
        private int part_id;
        private String part_name;

        public String getPart_name() {
            return part_name;
        }

        public void setPart_name(String part_name) {
            this.part_name = part_name;
        }

        public User() {

        }

        public User(int user_id, String name, int part_id) {
            this.user_id = user_id;
            this.name = name;
            this.part_id = part_id;
        }

        public int getUser_id() {
            return user_id;
        }

        public void setUser_id(int user_id) {
            this.user_id = user_id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPart_id() {
            return part_id;
        }

        public void setPart_id(int part_id) {
            this.part_id = part_id;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("user_id", user_id)
                    .append("name", name)
                    .append("part_id", part_id)
                    .append("part_name", part_name)
                    .toString();
        }
    }
}

