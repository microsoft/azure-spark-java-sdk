// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

import com.microsoft.azure.arm.resources.Region;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.Cluster;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ClusterGetProperties;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ClusterIdentity;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ConnectivityEndpoint;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.ClusterInner;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.HDInsightManager;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;

import java.util.Map;

/**
 * The HDInsight Cluster class.
 */
public final class HdiClusterDetail implements ClusterDetail, LivyCluster, YarnCluster, Cluster {
    private final boolean isGatewayRestAuthCredentialEnabled;
    private final Map<String, String> coreSiteConfig;
    private final Map<String, String> gatewayConf;
    private final Cluster cluster;

    public String getYarnUIBaseUrl() {
        return this.getConnectionUrl() + "/yarnui/";
    }

    public String getLivyConnectionUrl() {
        return this.getConnectionUrl() + "/livy/";
    }

    public String getYarnNMConnectionUrl() {
        return this.getYarnUIBaseUrl() + "ws/v1/clusters/apps/";
    }

    public String getName() {
        return cluster.name();
    }

    public String getTitle() {
        return this.getName() + " @" + this.regionName();
    }

    public String getConnectionUrl() {
        ConnectivityEndpoint httpConnEP = inner().properties().connectivityEndpoints()
                .stream()
                .filter(it -> StringUtils.equalsIgnoreCase(it.name(), "HTTPS"))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Can't find HTTPS connection entity from HDInsight cluster properties"));

        // such as https://hdicluster.azurehdinsight.net
        return httpConnEP.name().toLowerCase() + "://" + httpConnEP.location();
    }

    public boolean isGatewayRestAuthCredentialEnabled() {
        return this.isGatewayRestAuthCredentialEnabled;
    }

    @Nullable
    public String getHttpUserName() {
        return this.isGatewayRestAuthCredentialEnabled
                ? this.gatewayConf.get(GatewayRestAuthCredentialConfigKey.USERNAME.getKey())
                : null;
    }

    @Nullable
    public String getHttpPassword() {
        return this.isGatewayRestAuthCredentialEnabled
                ? this.gatewayConf.get(GatewayRestAuthCredentialConfigKey.PASSWORD.getKey())
                : null;
    }

    public HdiClusterDetail(Cluster cluster, Map<String, String> coreSiteConfig, Map<String, String> gatewayConf) {
        super();
        this.cluster = cluster;
        this.coreSiteConfig = coreSiteConfig;
        this.gatewayConf = gatewayConf;

        String isGWAuthCredEnabled = this.gatewayConf.get(GatewayRestAuthCredentialConfigKey.ENABLED.getKey());
        this.isGatewayRestAuthCredentialEnabled = isGWAuthCredEnabled != null
                && StringUtils.equalsIgnoreCase(isGWAuthCredEnabled, "true");
    }

    public String etag() {
        return this.cluster.etag();
    }

    public String id() {
        return this.cluster.id();
    }

    public ClusterIdentity identity() {
        return this.cluster.identity();
    }

    public ClusterInner inner() {
        return this.cluster.inner();
    }

    public String key() {
        return this.cluster.key();
    }

    public HDInsightManager manager() {
        return this.cluster.manager();
    }

    public String name() {
        return this.cluster.name();
    }

    public ClusterGetProperties properties() {
        return this.cluster.properties();
    }

    public Cluster refresh() {
        return this.cluster.refresh();
    }

    public Observable<Cluster> refreshAsync() {
        return this.cluster.refreshAsync();
    }

    public Region region() {
        return this.cluster.region();
    }

    public String regionName() {
        return this.cluster.regionName();
    }

    public String resourceGroupName() {
        return this.cluster.resourceGroupName();
    }

    public Map<String, String> tags() {
        return this.cluster.tags();
    }

    public String type() {
        return this.cluster.type();
    }

    public Update update() {
        return this.cluster.update();
    }

    private enum GatewayRestAuthCredentialConfigKey {
        ENABLED("restAuthCredential.isEnabled"),
        USERNAME("restAuthCredential.username"),
        PASSWORD("restAuthCredential.password");

        private final String key;

        public final String getKey() {
            return this.key;
        }

        GatewayRestAuthCredentialConfigKey(String key) {
            this.key = key;
        }
    }
}
