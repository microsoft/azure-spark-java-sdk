// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import com.microsoft.azure.spark.tools.http.UserAgentEntity;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Versions {
    public static final String UNKNOWN_VERSION = "UNKNOWN_VERSION";
    public static final String PROPERTIES_RESOURCE = "/properties";
    public static final String DEFAULT_ID = "azure-spark-java-sdk";
    public static final String OS = System.getProperty("os.name") + " " +
                                    System.getProperty("os.version") + " " +
                                    System.getProperty("os.arch");
    public static final String JAVA = System.getProperty("java.version");
    public static final String JVM = System.getProperty("java.vm.name") + " by " +
                                     System.getProperty("java.vm.vendor") + " " +
                                     System.getProperty("java.vm.version");
    public static final Lazy<UserAgentEntity> DEFAULT_USER_AGENT = new Lazy<>(() ->
            new UserAgentEntity.Builder(DEFAULT_ID)
                    .version(getVersion())
                    .comment("os", OS)
                    .comment("java", JAVA)
                    .comment("jre", JVM)
                    .build());

    public static Properties getProperties() {
        Properties properties = new Properties();

        try {
            InputStream propertiesInput = Versions.class.getResourceAsStream(PROPERTIES_RESOURCE);

            if (propertiesInput != null) {
                properties.load(propertiesInput);
            }
        } catch (IOException ignored) {
        }

        return properties;
    }

    public static String getVersion() {
        String versionFromProperties = getProperties().getProperty("version");

        if (versionFromProperties != null) {
            return versionFromProperties;
        }

        String versionFromManifest = Versions.class.getPackage().getImplementationVersion();

        return versionFromManifest == null ? UNKNOWN_VERSION : versionFromManifest;
    }

    public static String getId() {
        String id = getProperties().getProperty("id");

        return id == null ? DEFAULT_ID : id;
    }
}
