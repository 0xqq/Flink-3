package bean;

import com.alibaba.otter.canal.protocol.CanalEntry.Type;

import java.io.Serializable;
import java.util.List;

/**
 * 每一条数据的Bean类
 * 将会变成Json字符串发送到Kafka
 * @author lwj
 * @date 2018/2/2
 */
public class BinLogBean implements Serializable{
    private String instance;
    private int version;
    private Long serverId;
    private Type sourceType;
    private String executeTime;
    private String logfileName;
    private Long logfileOffset;
    /**
     * database name
     */
    private String schemaName;
    private String tableName;
    private String eventType;
    private List<ColumnField> columnFieldsList;


    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public List<ColumnField> getColumnFieldsList() {
        return columnFieldsList;
    }

    public void setColumnFieldsList(List<ColumnField> columnFieldsList) {
        this.columnFieldsList = columnFieldsList;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public Type getSourceType() {
        return sourceType;
    }

    public void setSourceType(Type sourceType) {
        this.sourceType = sourceType;
    }

    public String getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(String executeTime) {
        this.executeTime = executeTime;
    }

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public Long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(Long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

}