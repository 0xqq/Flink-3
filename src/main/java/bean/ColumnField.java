package bean;

import java.io.Serializable;

/**
 * 字段信息的Bean类
 * @author lwj
 * @date 2018/2/2
 */
public class ColumnField implements Serializable{
    /**
     *  columnName
     */
    private String name;
    private String mysqlType;
    private String value;
    private Boolean nullAble;
    private Boolean updated;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getMysqlType() {
        return mysqlType;
    }
    public void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }

    public Boolean getNullAble() {
        return nullAble;
    }

    public void setNullAble(Boolean nullAble) {
        this.nullAble = nullAble;
    }

    public Boolean getUpdated() {
        return updated;
    }
    public void setUpdated(Boolean updated) {
        this.updated = updated;
    }
}