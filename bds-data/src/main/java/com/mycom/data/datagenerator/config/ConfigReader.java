package com.mycom.data.datagenerator.config;

import java.io.File;
import java.net.URL;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

public class ConfigReader {
    private static final String CONFIG = "config.xml";
    private static DataConfiguration configModel = null;

    public static DataConfiguration getDataConfig() {
        if (null == configModel) {
            configModel = readConfigFile();
            System.out.println(configModel);
        }
        return configModel;
    }

    private static DataConfiguration readConfigFile() {
        DataConfiguration inputData = parseConfigFile(getConfigFile());
        return inputData;
    }

    private static File getConfigFile() {
        File file = new File(CONFIG);
        if (file.exists()) {
            System.out.println("Config file is '" + file.getAbsolutePath() + "'.");
        } else {
            URL resource = ConfigReader.class.getResource("/" + CONFIG);
            file = new File(resource.getFile());
            if (file.exists()) {
                System.out.println("Config file is '" + file.getAbsolutePath() + "'.");
            } else {
                throw new RuntimeException(
                        "File '" + file.getAbsolutePath() + "' does not exists. Please provide " + CONFIG);
            }
        }
        return file;
    }

    private static DataConfiguration parseConfigFile(File config) {
        Document document = getDocument(config);
        DataConfiguration configModel = new DataConfiguration();
        configModel.setNumberOfFiles(Integer.parseInt(get(document.selectSingleNode("//config/no-of-files"))));
        configModel.setNumberOfRecordsPerFile(
                Integer.parseInt(get(document.selectSingleNode("//config/no-of-records-perfile"))));

        configModel.setGgsn(Integer.parseInt(get(document.selectSingleNode("//config/ggsn"))));
        configModel.setApn(Integer.parseInt(get(document.selectSingleNode("//config/apn"))));

        configModel.setProtocolGroup(Integer.parseInt(get(document.selectSingleNode("//config/protocol-group"))));
        configModel.setProtocol(Integer.parseInt(get(document.selectSingleNode("//config/protocol"))));

        configModel.setUser(Integer.parseInt(get(document.selectSingleNode("//config/users"))));
        configModel.setData(Integer.parseInt(get(document.selectSingleNode("//config/data"))));
        configModel.setWebsite(Integer.parseInt(get(document.selectSingleNode("//config/website"))));

        return configModel;
    }

    private static String get(Node node) {
        return node.getText().trim();
    }

    private static Document getDocument(File file) {
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(file);
            return document;
        } catch (DocumentException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

}
