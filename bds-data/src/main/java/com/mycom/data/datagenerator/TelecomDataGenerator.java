package com.mycom.data.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.mycom.data.datagenerator.config.ConfigReader;
import com.mycom.data.datagenerator.config.DataConfiguration;
import com.mycom.data.datagenerator.model.MobileUserData;

public class TelecomDataGenerator {
    private DataConfiguration configData;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private DateFormat fileformat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-sss");

    public TelecomDataGenerator() {
        this.configData = ConfigReader.getDataConfig();
    }

    public static void main(String[] args) {
        TelecomDataGenerator dataGenerator = new TelecomDataGenerator();
        dataGenerator.generateData();
    }

    private void generateData() {
        System.out.println("Starting data generation.");
        int numberOfFiles = configData.getNumberOfFiles();
        for (int i = 0; i < numberOfFiles; i++) {
            System.out.println("Starting " + (i + 1) + "/" + numberOfFiles);
            List<MobileUserData> records = getRecords(configData.getNumberOfRecordsPerFile());
            writeToFile(records);
            System.out.println("Done " + (i + 1) + "/" + numberOfFiles);
        }
        System.out.println("Data generation are done");
    }

    private void writeToFile(List<MobileUserData> records) {
        StringBuilder builder = new StringBuilder(records.size() * 110);
        for (MobileUserData mobileUserData : records) {
            builder.append(mobileUserData.toString());
            builder.append("\n");
        }
        String fileName = "data" + fileformat.format(new Date()) + ".csv";
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(fileName);
            IOUtils.write(builder.toString(), fileWriter);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private List<MobileUserData> getRecords(long numberOfRecords) {
        List<MobileUserData> datas = new ArrayList<MobileUserData>();
        Date date = new Date();
        for (int i = 0; i < numberOfRecords; i++) {
            double random = Math.random();
            MobileUserData data = new MobileUserData();
            data.setDate(getDate(date, random));
            data.setGgsnId("ggsn" + (long) Math.ceil(configData.getGgsn() * random));
            data.setApnId("apn" + (long) Math.ceil(configData.getApn() * random));
            data.setProtocolGroup("protocolGroup" + (long) Math.ceil(configData.getProtocolGroup() * random));
            data.setProtocol("protocol" + (long) Math.ceil(configData.getProtocol() * random));
            data.setUserId("user" + (long) Math.ceil(configData.getUser() * random));
            data.setWebsite("website" + (long) Math.ceil(configData.getWebsite() * random));
            data.setUplinkVolume((long) Math.ceil(configData.getData() * Math.random()));
            data.setDownlinkVolume((long) Math.ceil(configData.getData() * Math.random()));
            data.setUplinkPackets((long) Math.ceil(configData.getData() * Math.random()));
            data.setDownlinkPackets((long) Math.ceil(configData.getData() * Math.random()));
            datas.add(data);
        }
        return datas;
    }

    private String getDate(Date date, double random) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MONTH, (int) Math.ceil(12 * random));
        cal.set(Calendar.MINUTE, (int) Math.ceil(60 * random));
        cal.set(Calendar.HOUR_OF_DAY, (int) Math.ceil(24 * random));
        return dateFormat.format(cal.getTime());
    }

}
