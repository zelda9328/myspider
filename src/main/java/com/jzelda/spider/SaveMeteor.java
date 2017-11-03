/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jzelda.spider;

import java.sql.ParameterMetaData;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import com.jzelda.util.MysqlProperty;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 *
 * @author engin
 */
class SaveMeteor {
    private String user,passwd,host,dbName,connArgs;
    Connection conn = null;
    
    public SaveMeteor(MysqlProperty args){
        connArgs = String.format("jdbc:mysql://%s/%s?characterEncoding=utf8"
                + "&generateSimpleParameterMetadata=true"
                + "&useServerPrepStmts=true", args.host, args.dbName);
        user = args.user;
        passwd = args.passwd;                
    }
    
    Boolean connect(){
        Boolean rs = false;
        try{
            conn = DriverManager.getConnection(connArgs, user, passwd);
        } catch(Exception e){
            e.printStackTrace();
        }
        
        rs = conn != null? true: false;
        return rs;
    }
    
    void close(){
        if(conn != null){
            try{
                conn.close();
            } catch(Exception e){
                
            }
        }
    }
    
    void write(String meteorData){
                if( conn == null) return;
        
        String[] dataUnit = meteorData.split(",");
        
        String sql = "insert into messure(obsTime,PRES,TEMP,HUMD,WDSD,WDIR,WS15M,WD15M,H_24R,SUN,id) "
                + "select distinct ?,?,?,?,?,?,?,?,?,?,? from dual "
                + "where not exists(select 1 from messure where id=? and obsTime = ?)";
        SimpleDateFormat withHour = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        try(PreparedStatement ps = conn.prepareStatement(sql)){
            ParameterMetaData meta = ps.getParameterMetaData();
            Date time = withHour.parse(dataUnit[0]);
            Timestamp stamp = new Timestamp(time.getTime());
            for(int i=1; i<dataUnit.length; i++){
                //日照可能會空字串
                if(dataUnit[i].isEmpty()){
                    int type = meta.getParameterType(i+1);
                    ps.setNull(i+1, type);
                    continue;
                }
                //首見降雨量為T
                if(dataUnit[i].equals("T")){
                    int type = meta.getParameterType(i+1);
                    ps.setNull(i+1, type);
                    continue;
                }
                //首見風向為V
                if(dataUnit[i].equals("V")){
                    int type = meta.getParameterType(i+1);
                    ps.setNull(i+1, type);
                    continue;
                }
                //首見淡水站，可能'H_24R', 'TEMP' ,'PRES' ,'WS15M' ,'HUMD' 
                if(dataUnit[i].equals("X")){
                    int type = meta.getParameterType(i+1);
                    ps.setNull(i+1, type);
                    continue;
                }
                ps.setString(i+1, dataUnit[i]);
            }
            ps.setTimestamp(1, stamp);
            ps.setString(12, dataUnit[10]);
            ps.setTimestamp(13, stamp);            
            ps.execute();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
