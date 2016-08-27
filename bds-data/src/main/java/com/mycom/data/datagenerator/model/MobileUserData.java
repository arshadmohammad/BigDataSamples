package com.mycom.data.datagenerator.model;

public class MobileUserData {
    private String date;
    private String ggsnId;
    private String apnId;
    private String protocolGroup;
    private String protocol;
    private String userId;
    private String website;
    private long uplinkVolume;
    private long downlinkVolume;
    private long uplinkPackets;
    private long downlinkPackets;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getGgsnId() {
        return ggsnId;
    }

    public void setGgsnId(String ggsnId) {
        this.ggsnId = ggsnId;
    }

    public String getApnId() {
        return apnId;
    }

    public void setApnId(String apnId) {
        this.apnId = apnId;
    }

    public String getProtocolGroup() {
        return protocolGroup;
    }

    public void setProtocolGroup(String protocolGroup) {
        this.protocolGroup = protocolGroup;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public long getUplinkVolume() {
        return uplinkVolume;
    }

    public void setUplinkVolume(long uplinkVolume) {
        this.uplinkVolume = uplinkVolume;
    }

    public long getDownlinkVolume() {
        return downlinkVolume;
    }

    public void setDownlinkVolume(long downlinkVolume) {
        this.downlinkVolume = downlinkVolume;
    }

    public long getUplinkPackets() {
        return uplinkPackets;
    }

    public void setUplinkPackets(long uplinkPackets) {
        this.uplinkPackets = uplinkPackets;
    }

    public long getDownlinkPackets() {
        return downlinkPackets;
    }

    public void setDownlinkPackets(long downlinkPackets) {
        this.downlinkPackets = downlinkPackets;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(date);
        builder.append(",");
        builder.append(ggsnId);
        builder.append(",");
        builder.append(apnId);
        builder.append(",");
        builder.append(protocolGroup);
        builder.append(",");
        builder.append(protocol);
        builder.append(",");
        builder.append(userId);
        builder.append(",");
        builder.append(website);
        builder.append(",");
        builder.append(uplinkVolume);
        builder.append(",");
        builder.append(downlinkVolume);
        builder.append(",");
        builder.append(uplinkPackets);
        builder.append(",");
        builder.append(downlinkPackets);
        return builder.toString();
    }

}
