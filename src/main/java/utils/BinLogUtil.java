package utils;

import bean.BinLogBean;
import bean.ColumnField;

import java.util.List;

/**
 * 处理kafka binlog日志的工具类
 * @author lwj
 * @date 2018/11/23
 */
public class BinLogUtil {
    /**
     * 通过对应的filed获取bean中对应的值
     * @param binLogBean
     * @param field
     * @return
     */
    public static String getValueByField(BinLogBean binLogBean, String field){
        List<ColumnField> columnFieldsList = binLogBean.getColumnFieldsList();
        for (ColumnField cf : columnFieldsList){
            if (field.equals(cf.getName())){
                return cf.getValue();
            }
        }
        return "";
    }

    /**
     * 通过对应的filed获取bean中对应的值，并且判断该field是否有更新
     * @param binLogBean
     * @param field
     * @param update
     * @return
     */
    public static String getValueByFieldWithUpdate(BinLogBean binLogBean, String field, boolean update){
        List<ColumnField> columnFieldsList = binLogBean.getColumnFieldsList();
        for (ColumnField cf : columnFieldsList){
            if (field.equals(cf.getName()) && update == cf.getUpdated()){
                return cf.getValue();
            }
        }
        return "";
    }
}