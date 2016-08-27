package com.mycom.data.datagenerator.config;

public class DataConfiguration {
    private int numberOfFiles;
    private long numberOfRecordsPerFile;
    private int ggsn;
    private int apn;
    private int protocolGroup;
    private int protocol;
    private int user;
    private int data;
    // number of web site in the world
    private int website;

    public int getNumberOfFiles() {
        return numberOfFiles;
    }

    public void setNumberOfFiles(int numberOfFiles) {
        this.numberOfFiles = numberOfFiles;
    }

    public long getNumberOfRecordsPerFile() {
        return numberOfRecordsPerFile;
    }

    public void setNumberOfRecordsPerFile(long numberOfRecordsPerFile) {
        this.numberOfRecordsPerFile = numberOfRecordsPerFile;
    }

    public int getGgsn() {
        return ggsn;
    }

    public void setGgsn(int ggsn) {
        this.ggsn = ggsn;
    }

    public int getApn() {
        return apn;
    }

    public void setApn(int apn) {
        this.apn = apn;
    }

    public int getProtocolGroup() {
        return protocolGroup;
    }

    public void setProtocolGroup(int protocolGroup) {
        this.protocolGroup = protocolGroup;
    }

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }

    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }

    public int getWebsite() {
        return website;
    }

    public void setWebsite(int website) {
        this.website = website;
    }

    @Override
    public String toString() {
        return "DataConfiguration [numberOfFiles=" + numberOfFiles + ", numberOfRecordsPerFile="
                + numberOfRecordsPerFile + ", ggsn=" + ggsn + ", apn=" + apn + ", protocolGroup=" + protocolGroup
                + ", protocol=" + protocol + ", user=" + user + ", data=" + data + ", website=" + website + "]";
    }
}
