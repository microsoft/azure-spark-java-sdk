// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.livy.batches.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.spark.tools.restapi.Convertible;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class PostBatches implements Convertible {
    /*
     * For interactive spark job:
     *
     * kind             The session kind (required) session kind
     * proxyUser        The user to impersonate that will run this session (e.g. bob)                   string
     * jars             Files to be placed on the java classpath                                        list of paths
     * pyFiles          Files to be placed on the PYTHONPATH                                            list of paths
     * files            Files to be placed in executor working directory                                list of paths
     * driverMemory     Memory for driver (e.g. 1000M, 2G)                                              string
     * driverCores      Number of cores used by driver (YARN mode only)                                 int
     * executorMemory   Memory for executor (e.g. 1000M, 2G)                                            string
     * executorCores    Number of cores used by executor                                                int
     * numExecutors     Number of executors (YARN mode only)                                            int
     * archives         Archives to be uncompressed in the executor working directory (YARN mode only)  list of paths
     * queue            The YARN queue to submit too (YARN mode only)                                   string
     * name             Name of the application                                                         string
     * conf             Spark configuration properties                                                  Map of key=val
     */
    private String name = "";
    private String file = "";
    private String className = "";

    private String clusterName = "";
    private boolean isLocalArtifact = false;
    private String artifactName = "";
    private String localArtifactPath = "";
    private List<String> files = new ArrayList<>();
    private List<String> jars = new ArrayList<>();
    private List<String> args = new ArrayList<>();
    private Map<String, Object> jobConfig = new HashMap<>();

    private static final Pattern memorySizeRegex = Pattern.compile("\\d+(.\\d+)?[gGmM]");

    public static final String DriverMemory = "driverMemory";
    public static final String DriverMemoryDefaultValue = "4G";

    public static final String DriverCores = "driverCores";
    public static final int DriverCoresDefaultValue = 1;

    public static final String ExecutorMemory = "executorMemory";
    public static final String ExecutorMemoryDefaultValue = "4G";

    public static final String NumExecutors = "numExecutors";
    public static final int NumExecutorsDefaultValue = 5;

    public static final String ExecutorCores = "executorCores";
    public static final int ExecutorCoresDefaultValue = 1;

    public static final String Conf = "conf";   //     Spark configuration properties

    public static final String NAME = "name";

    public PostBatches() {
    }

    public PostBatches(String clusterName,
                       boolean isLocalArtifact,
                       String artifactName,
                       String localArtifactPath,
                       String filePath,
                       String className,
                       List<String> referencedFiles,
                       List<String> referencedJars,
                       List<String> args,
                       Map<String, Object> jobConfig) {
        this.clusterName = clusterName;
        this.isLocalArtifact = isLocalArtifact;
        this.artifactName = artifactName;
        this.localArtifactPath = localArtifactPath;
        this.file = filePath;
        this.className = className;
        this.files = referencedFiles;
        this.jars = referencedJars;
        this.jobConfig = jobConfig;
        this.args = args;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setLocalArtifact(boolean localArtifact) {
        isLocalArtifact = localArtifact;
    }

    public void setArtifactName(String artifactName) {
        this.artifactName = artifactName;
    }

    @JsonIgnore
    public String getClusterName() {
        return clusterName;
    }

    @JsonIgnore
    public boolean isLocalArtifact() {
        return isLocalArtifact;
    }

    @JsonIgnore
    public String getArtifactName() {
        return artifactName;
    }

    @JsonIgnore
    @Nullable
    public String getLocalArtifactPath() {
        return localArtifactPath;
    }

    public void setLocalArtifactPath(String path) {
        localArtifactPath = path;
    }

    @JsonProperty(NAME)
    public String getName() {
        return name;
    }

    @JsonProperty("file")
    @Nullable
    public String getFile() {
        return file;
    }

    @JsonProperty("className")
    public String getMainClassName() {
        return className;
    }

    @JsonProperty("files")
    @Nullable
    public List<String> getReferencedFiles() {
        return files;
    }

    @JsonProperty("jars")
    @Nullable
    public List<String> getReferencedJars() {
        return jars;
    }

    @JsonProperty("args")
    @Nullable
    public List<String> getArgs() {
        return args;
    }

    @JsonIgnore
    @Nullable
    public Map<String, Object> getJobConfig() {
        return jobConfig;
    }

    public void setFilePath(String filePath) {
        this.file = filePath;
    }

    @JsonProperty("driverMemory")
    @Nullable
    public String getDriverMemory() {
        return (String) jobConfig.get(DriverMemory);
    }

    @JsonProperty("driverCores")
    @Nullable
    public Integer getDriverCores() {
        return parseIntegerSafety(jobConfig.get(DriverCores));
    }

    @JsonProperty("executorMemory")
    @Nullable
    public String getExecutorMemory() {
        return (String) jobConfig.get(ExecutorMemory);
    }

    @JsonProperty("executorCores")
    @Nullable
    public Integer getExecutorCores() {
        return parseIntegerSafety(jobConfig.get(ExecutorCores));
    }

    @JsonProperty("numExecutors")
    @Nullable
    public Integer getNumExecutors() {
        return parseIntegerSafety(jobConfig.get(NumExecutors));
    }

    @JsonProperty("conf")
    @Nullable
    public Map<String, String> getConf() {
        Map<String, String> jobConf = new HashMap<>();

        Optional.ofNullable(jobConfig.get(Conf))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .ifPresent(conf -> conf.forEach((k, v) -> jobConf.put((String) k, (String) v)));

        return jobConf.isEmpty() ? null : jobConf;
    }

    @JsonIgnore
    @Nullable
    private Integer parseIntegerSafety(@Nullable Object maybeInteger) {
        if (maybeInteger == null) {
            return null;
        }

        if (maybeInteger instanceof Integer) {
            return (Integer) maybeInteger;
        }

        try {
            return Integer.parseInt(maybeInteger.toString());
        } catch (Exception ignored) {
            return null;
        }
    }

    public List<Pair<String, String>> flatJobConfig() {
        List<Pair<String, String>> flattedConfigs = new ArrayList<>();

        Map<String, Object> jobConf = getJobConfig();

        if (jobConf == null) {
            return Collections.emptyList();
        }

        jobConf.forEach((key, value) -> {
            if (isSubmissionParameter(key)) {
                flattedConfigs.add(Pair.of(key, value == null ? null : value.toString()));
            } else if (key.equals(Conf)) {
                new SparkConfigures(value).forEach((scKey, scValue) ->
                        flattedConfigs.add(Pair.of(scKey, scValue == null ? null : scValue.toString())));
            }
        });

        return flattedConfigs;
    }

    public void applyFlattedJobConf(final List<Pair<String, String>> jobConfFlatted) {
        jobConfig.clear();

        SparkConfigures sparkConfig = new SparkConfigures();

        jobConfFlatted.forEach(kvPair -> {
            if (isSubmissionParameter(kvPair.getLeft())) {
                jobConfig.put(kvPair.getLeft(), kvPair.getRight());
            } else {
                sparkConfig.put(kvPair.getLeft(), kvPair.getRight());
            }
        });

        if (!sparkConfig.isEmpty()) {
            jobConfig.put(Conf, sparkConfig);
        }
    }

    public String serializeToJson() {
        return convertToJson().orElse("");
    }

    public static final String[] parameterList = new String[] {
            PostBatches.DriverMemory,
            PostBatches.DriverCores,
            PostBatches.ExecutorMemory,
            PostBatches.ExecutorCores,
            PostBatches.NumExecutors
    };

    /**
     * Checks whether the key is one of Spark Job submission parameters or not.
     *
     * @param key the key string to check
     * @return true if the key is a member of submission parameters; false otherwise
     */
    public static boolean isSubmissionParameter(String key) {
        return Arrays.stream(PostBatches.parameterList).anyMatch(key::equals);
    }
}