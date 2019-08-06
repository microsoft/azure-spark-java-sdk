// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.livy.batches.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.spark.tools.functions.StringAction1;
import com.microsoft.azure.spark.tools.restapi.Convertible;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Class for POST body to submit a Spark Batch application into Livy service with the following JSON fields
 *   file             File containing the application to execute                                      string
 *   proxyUser        The user to impersonate that will run this session (e.g. bob)                   string
 *   className        Application Java/Spark main class                                               string
 *   args             Command line arguments for the application                                      list of strings
 *   jars             jars to be used in this session                                                 list of strings
 *   pyFiles          Files to be placed on the PYTHONPATH                                            list of strings
 *   files            Files to be placed in executor working directory                                list of strings
 *   driverMemory     Memory for driver (e.g. 1000M, 2G)                                              string
 *   driverCores      Number of cores used by driver (YARN mode only)                                 int
 *   executorMemory   Memory for executor (e.g. 1000M, 2G)                                            string
 *   executorCores    Number of cores used by executor                                                int
 *   numExecutors     Number of executors (YARN mode only)                                            int
 *   archives         Archives to be uncompressed in the executor working directory (YARN mode only)  list of paths
 *   queue            The YARN queue to submit too (YARN mode only)                                   string
 *   name             Name of the application                                                         string
 *   conf             Spark configuration properties                                                  Map of key=val
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class PostBatches implements Convertible {
    public static class MemorySize {
        private static final Pattern memorySizeRegex = Pattern.compile("\\d+(.\\d+)?[gGmM]");

        public enum Unit {
            MEGABYTES("M"),
            GIGABYTES("G");

            private final String present;

            Unit(final String present) {
                this.present = present;
            }


            @Override
            public String toString() {
                return this.present;
            }
        }

        final String value;

        /**
         * Constructor with memory size string with unit.
         *
         * @param memorySize memory size string with unit, such like 1G, 800m or 1.5g
         */
        public MemorySize(final String memorySize) {
            if (!memorySizeRegex.matcher(memorySize).matches()) {
                throw new IllegalArgumentException("Unsupported memory size format: " + memorySize
                        + " , which should be like 1G, 800m or 1.5g");
            }

            this.value = memorySize;
        }

        public MemorySize(final float value, final Unit unit) {
            this.value = ((value == (int) value) ? Integer.toString((int) value) : value) + unit.toString();
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(final @Nullable Object obj) {
            if (!(obj instanceof MemorySize)) {
                return false;
            }

            return value.equals(obj);
        }
    }

    public static class Options {
        private @Nullable String name = null;

        private @Nullable String proxyUser = null;

        private @Nullable String artifactUri = null;

        private @Nullable String className = null;

        private List<String> referenceFiles = new ArrayList<>();

        private List<String> referencedJars = new ArrayList<>();

        private List<String> args = new ArrayList<>();

        private List<String> pyFiles = new ArrayList<>();

        private List<String> yarnArchives = new ArrayList<>();

        private Map<String, String> jobConfig = new HashMap<>();

        private @Nullable String yarnQueue = null;

        private @Nullable MemorySize driverMemory = null;

        private @Nullable Integer driverCores = null;

        private @Nullable MemorySize executorMemory = null;

        private @Nullable Integer executorCores = null;

        private @Nullable Integer yarnNumExecutors = null;

        /**
         * Set Spark application name.
         *
         * @param appName application name to set
         * @return current {@link Options} instance for fluent calling
         */
        public Options name(final String appName) {
            this.name = appName;

            return this;
        }

        /**
         * Set Spark application proxy user.
         *
         * @param user proxy user name to set
         * @return current {@link Options} instance for fluent calling
         */
        public Options proxyUser(final String user) {
            this.proxyUser = user;

            return this;
        }

        /**
         * Set Spark application artifact URI to find main class.
         *
         * @param uri application artifact URI to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options artifactUri(final String uri) {
            this.artifactUri = uri;

            return this;
        }

        /**
         * Set Spark application main class to start.
         *
         * @param mainClassName application main class to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options className(final String mainClassName) {
            this.className = mainClassName;

            return this;
        }

        /**
         * Set Spark application reference files.
         *
         * @param files application referece files to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options referFiles(final String... files) {
            Collections.addAll(this.referenceFiles, files);

            return this;
        }

        /**
         * Set Spark application reference Jar files.
         *
         * @param jars application reference Jar files to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options referJars(final String... jars) {
            Collections.addAll(this.referencedJars, jars);

            return this;
        }

        /**
         * Set Spark application arguments.
         *
         * @param jobArgs application arguments to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options args(final String... jobArgs) {
            Collections.addAll(this.args, jobArgs);

            return this;
        }

        /**
         * Set Spark application Yarn archives.
         *
         * @param archives Yarn archives to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options yarnArchives(final String... archives) {
            Collections.addAll(this.yarnArchives, archives);

            return this;
        }

        /**
         * Set Spark application configuration.
         *
         * @param key key for Spark configuration to set into option
         * @param value value for Spark configuration to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options conf(final String key, final String value) {
            if (exclusiveConfKeyActions.containsKey(key)) {
                exclusiveConfKeyActions.get(key).invoke(value);
            } else {
                jobConfig.put(key, value);
            }

            return this;
        }

        /**
         * Set Spark application configuration.
         *
         * @param kvPairs key-value pairs for Spark configuration to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options confs(final Pair<String, String>... kvPairs) {
            for (Pair<String, String> kv : kvPairs) {
                jobConfig.put(kv.getKey(), kv.getValue());
            }

            return this;
        }

        /**
         * Set Spark application Yarn Driver memory size to allocate.
         *
         * @param size Yarn Driver memory size string with unit to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setDriverMemory(final String size) {
            this.driverMemory = new MemorySize(size);

            return this;
        }

        /**
         * Set Spark application Yarn Driver memory size to allocate.
         *
         * @param size Yarn Driver memory size to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setDriverMemory(final MemorySize size) {
            this.driverMemory = size;

            return this;
        }

        /**
         * Set Spark application Yarn Driver cores to allocate.
         *
         * @param cores Yarn Driver cores to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setDriverCores(final int cores) {
            this.driverCores = cores;

            return this;
        }

        /**
         * Set Spark application Yarn Executor memory size to allocate.
         *
         * @param size Yarn Executor memory size string with unit to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setExecutorMemory(final String size) {
            this.executorMemory = new MemorySize(size);

            return this;
        }

        /**
         * Set Spark application Yarn Executor memory size to allocate.
         *
         * @param size Yarn Executor memory size to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setExecutorMemory(final MemorySize size) {
            this.executorMemory = size;

            return this;
        }

        /**
         * Set Spark application Yarn Executor cores to allocate.
         * @param cores Yarn Executor cores to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setExecutorCores(final int cores) {
            this.executorCores = cores;

            return this;
        }

        /**
         * Set Spark application Yarn Executor number to allocate.
         * @param number Yarn Executor number to set into option
         * @return current {@link Options} instance for fluent calling
         */
        public Options setYarnNumExecutors(final int number) {
            this.yarnNumExecutors = number;

            return this;
        }

//        public Options withDefaultExecutorCountMemoryAndCoreSettings() {
//            this.
//            this.executorMemory = EXECUTOR_MEMORY_DEFAULT_VALUE;
//            return this;
//        }

        /**
         * Apply from a PostBatches instance.
         * @param postBatches a PostBatches instance to apply
         * @return current {@link Options} instance for fluent calling
         */
        public Options apply(final PostBatches postBatches) {
            this.name = postBatches.name;
            this.yarnQueue = postBatches.yarnQueue;
            this.className = postBatches.className;
            this.artifactUri = postBatches.file;
            this.proxyUser = postBatches.proxyUser;
            this.driverMemory = postBatches.driverMemory;
            this.driverCores = postBatches.driverCores;
            this.executorMemory = postBatches.executorMemory;
            this.executorCores = postBatches.executorCores;
            this.yarnNumExecutors = postBatches.yarnNumExecutors;

            this.referenceFiles.clear();
            this.referenceFiles.addAll(postBatches.files);

            this.referencedJars.clear();
            this.referencedJars.addAll(postBatches.jars);

            this.yarnArchives.clear();
            this.yarnArchives.addAll(postBatches.archives);

            this.pyFiles.clear();
            this.pyFiles.addAll(postBatches.pyFiles);

            this.args.clear();
            this.args.addAll(postBatches.args);

            this.jobConfig.clear();
            this.jobConfig.putAll(postBatches.jobConfig);

            return this;
        }

        private final Map<String, StringAction1> exclusiveConfKeyActions = Collections.unmodifiableMap(
                new HashMap<String, StringAction1>() {
                    {
                        put(PostBatches.DRIVER_MEMORY, size -> setDriverMemory(size));
                        put(PostBatches.DRIVER_CORES, cores -> setDriverCores(Integer.parseInt(cores)));
                        put(PostBatches.EXECUTOR_MEMORY, size -> setExecutorMemory(size));
                        put(PostBatches.EXECUTOR_CORES, cores -> setExecutorCores(Integer.parseInt(cores)));
                        put(PostBatches.NUM_EXECUTORS, num -> setYarnNumExecutors(Integer.parseInt(num)));
                    }
                });

        /**
         * Build POST Set Spark application name.
         * @return current {@link PostBatches} instance Post body
         */
        public PostBatches build() {
            String file = this.artifactUri;
            String mainClassName = this.className;

            if (StringUtils.isBlank(file)) {
                throw new IllegalArgumentException("Can't find Spark job artifact URI or local artifact to submit");
            }

            if (StringUtils.isBlank(mainClassName)) {
                throw new IllegalArgumentException("Can't find Spark job main class name to submit");
            }

            return new PostBatches(
                    this.name,
                    this.proxyUser,
                    file,
                    mainClassName,
                    this.yarnQueue,
                    this.driverMemory,
                    this.driverCores,
                    this.executorMemory,
                    this.executorCores,
                    this.yarnNumExecutors,
                    this.referenceFiles,
                    this.referencedJars,
                    this.yarnArchives,
                    this.pyFiles,
                    this.args,
                    this.jobConfig);
        }
    }

    private @Nullable String name;
    private @Nullable String proxyUser;
    private String file;
    private String className;

    private final List<String> files = new ArrayList<>();
    private final List<String> jars = new ArrayList<>();
    private final List<String> args = new ArrayList<>();
    private final Map<String, String> jobConfig = new HashMap<>();
    private final List<String> pyFiles = new ArrayList<>();
    private final List<String> archives = new ArrayList<>();

    private @Nullable String yarnQueue;

    private @Nullable MemorySize driverMemory;

    private @Nullable Integer driverCores;

    private @Nullable MemorySize executorMemory;

    private @Nullable Integer executorCores;

    private @Nullable Integer yarnNumExecutors;

    private static final String DRIVER_MEMORY = "driverMemory";
    public static final String DRIVER_MEMORY_DEFAULT_VALUE = "4G";

    private static final String DRIVER_CORES = "driverCores";
    public static final int DRIVER_CORES_DEFAULT_VALUE = 1;

    private static final String EXECUTOR_MEMORY = "executorMemory";
    public static final String EXECUTOR_MEMORY_DEFAULT_VALUE = "4G";

    private static final String NUM_EXECUTORS = "numExecutors";
    public static final int NUM_EXECUTORS_DEFAULT_VALUE = 5;

    private static final String EXECUTOR_CORES = "executorCores";
    public static final int EXECUTOR_CORES_DEFAULT_VALUE = 1;

    private PostBatches() {
        this.file = "<unset>";
        this.className = "<unset>";
    }

    protected PostBatches(final @Nullable String name,
                          final @Nullable String proxyUser,
                          final String filePath,
                          final String className,
                          final @Nullable String yarnQueue,
                          final @Nullable MemorySize driverMemory,
                          final @Nullable Integer driverCores,
                          final @Nullable MemorySize executorMemory,
                          final @Nullable Integer executorCores,
                          final @Nullable Integer yarnNumExecutors,
                          final @Nullable List<String> referencedFiles,
                          final @Nullable List<String> referencedJars,
                          final @Nullable List<String> archives,
                          final @Nullable List<String> pyFiles,
                          final @Nullable List<String> args,
                          final @Nullable Map<String, String> jobConfig) {
        this.name = name;
        this.file = filePath;
        this.className = className;
        this.proxyUser = proxyUser;
        this.yarnQueue = yarnQueue;
        this.driverMemory = driverMemory;
        this.driverCores = driverCores;
        this.executorMemory = executorMemory;
        this.executorCores = executorCores;
        this.yarnNumExecutors = yarnNumExecutors;

        if (referencedFiles != null) {
            this.files.addAll(referencedFiles);
        }

        if (referencedJars != null) {
            this.jars.addAll(referencedJars);
        }

        if (jobConfig != null) {
            this.jobConfig.putAll(jobConfig);
        }

        if (args != null) {
            this.args.addAll(args);
        }

        if (archives != null) {
            this.archives.addAll(archives);
        }

        if (pyFiles != null) {
            this.pyFiles.addAll(pyFiles);
        }
    }

    @JsonProperty("name")
    public @Nullable String getName() {
        return name;
    }

    @JsonProperty("file")
    public @Nullable String getFile() {
        return file;
    }

    @JsonProperty("className")
    public String getClassName() {
        return className;
    }

    @JsonProperty("files")
    public @Nullable List<String> getReferencedFiles() {
        return files;
    }

    @JsonProperty("jars")
    public @Nullable List<String> getReferencedJars() {
        return jars;
    }

    @JsonProperty("args")
    public @Nullable List<String> getArgs() {
        return args;
    }

    @JsonProperty("archives")
    public @Nullable List<String> getArchives() {
        return archives;
    }

    @JsonProperty("driverMemory")
    public @Nullable String getDriverMemory() {
        return driverMemory == null ? null : driverMemory.toString();
    }

    @JsonProperty("driverCores")
    public @Nullable Integer getDriverCores() {
        return driverCores;
    }

    @JsonProperty("executorMemory")
    public @Nullable String getExecutorMemory() {
        return executorMemory == null ? null : executorMemory.toString();
    }

    @JsonProperty("executorCores")
    public @Nullable Integer getExecutorCores() {
        return executorCores;
    }

    @JsonProperty("numExecutors")
    public @Nullable Integer getNumExecutors() {
        return yarnNumExecutors;
    }

    @JsonProperty("conf")
    public @Nullable Map<String, String> getConf() {
        return jobConfig.isEmpty() ? null : jobConfig;
    }

    @JsonProperty("proxyUser")
    public @Nullable String getProxyUser() {
        return proxyUser;
    }

    @JsonProperty("pyFiles")
    public @Nullable List<String> getPyFiles() {
        return pyFiles;
    }

    @JsonProperty("queue")
    public @Nullable String getYarnQueue() {
        return yarnQueue;
    }

    /**
     * Checks whether the key is one of Spark Job submission parameters or not.
     *
     * @param key the key string to check
     * @return true if the key is a member of submission parameters; false otherwise
     */
    public static boolean isSubmissionParameter(String key) {
        final String[] parameterList = new String[] {
                PostBatches.DRIVER_MEMORY,
                PostBatches.DRIVER_CORES,
                PostBatches.EXECUTOR_MEMORY,
                PostBatches.EXECUTOR_CORES,
                PostBatches.NUM_EXECUTORS
        };

        return Arrays.stream(parameterList).anyMatch(key::equals);
    }
}