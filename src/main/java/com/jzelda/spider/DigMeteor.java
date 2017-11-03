/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jzelda.spider;

import com.github.abola.crawler.CrawlerPack;
import java.io.BufferedInputStream;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import com.jzelda.util.MysqlProperty;

/**
 *
 * @author engin
 */
public class DigMeteor {
    public static void main(String[] args) throws Exception{
        Calendar now = Calendar.getInstance();
        String stTime="", endTime="";
        if(args.length != 0){            
            if(args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("-help")){
            System.out.println("本程式是抓取氣象觀測查詢系統，並儲存下來\n"
                    + "網址來源：http://e-service.cwb.gov.tw/HistoryDataQuery\n"
                    + "syntax: [endtime] [starttime]\n"
                    + "default is current day.\n"
                    + "time format:yyyy-mm-dd");
            
            return;
            }
            
            endTime = args.length == 2? args[0] : "";
            stTime = args.length == 2? args[1] : "";
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd");
        
        if(endTime.equals("")){
            stTime = sdf.format(now.getTime());
            now.add(Calendar.DATE, 1);
            endTime = sdf.format(now.getTime());
        } else{
            if(stTime.equals("")){
                System.out.println("Please check format of syntax.");
                return;
            }
        }
        
        DigMeteor dm = new DigMeteor();
        MysqlProperty sqlArgs = new MysqlProperty(dm, "/resource.xml");
        
        Connection conn = DriverManager.getConnection(sqlArgs.connectArgs, sqlArgs.user, sqlArgs.passwd);        
        
        //只有編號4開頭的站台才有日照資料
        String getStation = "select id,name from station where id like '4%'";
        PreparedStatement ps = conn.prepareStatement(getStation);
        ResultSet rs = ps.executeQuery();
        
        SaveMeteor saveMT = new SaveMeteor(sqlArgs);
        saveMT.connect();

        while(rs.next()){
            String nameUrl=rs.getString("name");
            for(int i=0; i<2; i++){
                nameUrl = URLEncoder.encode(nameUrl, "utf-8");
            }
            
            SimpleDateFormat withHour = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar copy;
            
            Calendar cal = Calendar.getInstance();
            Date firstDate = sdf.parse(stTime);
            Date endDate = sdf.parse(endTime);
            while(firstDate.before(endDate)){
                System.out.println("station: "+ rs.getString("name")+ " date page: "+  sdf.format(firstDate));
                cal.setTime(firstDate);                
                //System.out.println(sdf.format(cal.getTime()));
                String date_str = sdf.format(cal.getTime());
                String url = String.format("http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do"+
                    "?command=viewMain&station=%s&stname=%s&datepicker=%s",
                    rs.getString("id"), nameUrl, date_str);
                
                
                Elements rows = CrawlerPack.start().getFromHtml(url).select("#MyTable>tbody>tr");
                //SaveMeteorBycwb saveMeteor = new SaveMeteorBycwb();
                int index = -1;
                for(Element row : rows){
                    if( index < 1){
                        index ++;
                        continue;
                    }
                    copy = (Calendar)cal.clone();
                    copy.add(Calendar.HOUR, index);
            
                    StringBuilder full_data = new StringBuilder(withHour.format(copy.getTime()));                    
                    full_data.append(",");
                    full_data.append(ParseMeteor.parse(row.text())).append(",");
                    full_data.append(rs.getString("id"));
                    
                    //System.out.println(full_data.toString());                    
                    
                    saveMT.write(full_data.toString());
                    
                    index++;
                }
                
                cal.add(Calendar.DATE,1);
                firstDate = cal.getTime();
            }
        }
        
        rs.close();
        saveMT.close();
        ps.close();
        conn.close();
    }
}
