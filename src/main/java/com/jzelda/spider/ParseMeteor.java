/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jzelda.spider;

/**
 *
 * @author engin
 */
public class ParseMeteor {
        public static String parse(String data){
        String[] dataUnit = data.split(" ");
        final String separatemark = ",";
        //來源網址欄位有空格，這不是ascii的空格，要刪除
        for(int i=0; i<dataUnit.length; i++){
            dataUnit[i] = dataUnit[i].replace("\u00a0", "");
        }
        
        StringBuilder newData = new StringBuilder();
        //加入氣壓、氣溫、溼度
        for(int i=1; i<6; i+=2){
            newData.append(dataUnit[i]).append(separatemark);
        }
        //加入風速、風向、最大陣風、最大陣風風向、降水量
        for(int i=6; i<11 ; i++){
            newData.append(dataUnit[i]).append(separatemark);
        }
        //加入日照時數
        newData.append(dataUnit[12]);
        
        return newData.toString();
    }
}
