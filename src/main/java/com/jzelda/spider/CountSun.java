/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jzelda.spider;

import com.jzelda.util.MysqlProperty;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author engin
 */
public class CountSun extends SaveMeteor{
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
    public CountSun(MysqlProperty args){
        super(args);
    }
    
    public static void main(String args[]){
        MysqlProperty sqlArgs = new MysqlProperty(CountSun.class, "/resource.xml");
        CountSun cs = new CountSun(sqlArgs);
        Calendar cal = Calendar.getInstance();
        //cal.add(Calendar.DATE, -1);
        String today = sdf.format(cal.getTime());
        
        cs.connect();
        Date rec = cs.getLastTimeRec();
        cal.setTime(rec);
        cal.add(Calendar.DATE, -5);        
        
        cs.count(sdf.format(cal.getTime()), today);
    }
    
    public Date getLastTimeRec(){
        Date lastDate = null;
        
        try{
            lastDate = sdf.parse("2014-01-01");
        }catch(Exception e){
            
        }
        
        if(conn != null){
            String sql = "select calendar from messureByDay order by calendar desc limit 1";
            try(PreparedStatement ps = conn.prepareStatement(sql)){
                ResultSet rs = ps.executeQuery();
                while(rs.next()){
                    lastDate = rs.getDate(1);
                }
            } catch (Exception e){
                
            }
        }
        
        return lastDate;
    }
 
    private void count(String lastrec, String today){
        String delmessureByDay_Period = "delete from messureByDay "
                + "where calendar >= ? and calendar < ?";
        
        String countSyntax = "insert into messureByDay(calendar,sun,temp,id) "
                + "select obsTime,sum(sun), max(temp),id from messure "
                + "where obsTime >= ? and obsTime < ? and sun is not null group by DATE(obsTime), id";
        
        if(conn != null){
            try(PreparedStatement delPs = conn.prepareStatement(delmessureByDay_Period);
                PreparedStatement ps = conn.prepareStatement(countSyntax)
            ){
                delPs.setString(1, lastrec);
                delPs.setString(2, today);
                delPs.execute();
                
                ps.setString(1, lastrec);
                ps.setString(2, today);
                ps.execute();                
            } catch (SQLException ex) {
                Logger.getLogger(CountSun.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
