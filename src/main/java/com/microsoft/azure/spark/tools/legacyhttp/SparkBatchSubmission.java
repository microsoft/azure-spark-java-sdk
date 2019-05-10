// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.legacyhttp;

import com.microsoft.azure.spark.tools.http.HttpResponse;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.JsonConverter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SparkBatchSubmission implements Logger {
    SparkBatchSubmission() {
    }

    // Singleton Instance
    private static @Nullable SparkBatchSubmission instance = null;

    public static SparkBatchSubmission getInstance() {
        if (instance == null) {
            synchronized (SparkBatchSubmission.class) {
                if (instance == null) {
                    instance = new SparkBatchSubmission();
                }
            }
        }

        return instance;
    }

    private CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    //    //    public String getInstallationID() {
    //        if (HDInsightLoader.getHDInsightHelper() == null) {
    //            return "";
    //        }
    //
    //        return HDInsightLoader.getHDInsightHelper().getInstallationId();
    //    }

    public CloseableHttpClient getHttpClient() throws IOException {
        // TrustStrategy ts = ServiceManager.getServiceProvider(TrustStrategy.class);
        // SSLConnectionSocketFactory sslSocketFactory = null;
        //
        // if (ts != null) {
        //     try {
        //         SSLContext sslContext = new SSLContextBuilder()
        //                 .loadTrustMaterial(ts)
        //                 .build();
        //
        //         sslSocketFactory = new SSLConnectionSocketFactory(sslContext,
        //                 HttpObservable.isSSLCertificateValidationDisabled()
        //                         ? NoopHostnameVerifier.INSTANCE
        //                         : new DefaultHostnameVerifier());
        //     } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        //         log().error("Prepare SSL Context for HTTPS failure. " + ExceptionUtils.getStackTrace(e));
        //     }
        // }

        return HttpClients.custom()
                .useSystemProperties()
                // .setSSLSocketFactory(sslSocketFactory)
                // .setDefaultCredentialsProvider(credentialsProvider)
                .build();
    }

    /**
     * Set http request credential using username and password.
     *
     * @param username : username
     * @param password : password
     */
    public void setUsernamePasswordCredential(String username, String password) {
        credentialsProvider.setCredentials(new AuthScope(AuthScope.ANY),
                                           new UsernamePasswordCredentials(username, password));
    }

    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public HttpResponse getHttpResponseViaGet(String connectUrl) throws IOException {
        CloseableHttpClient httpclient = getHttpClient();

        HttpGet httpGet = new HttpGet(connectUrl);
        httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("User-Agent", getUserAgentPerRequest(false));
        httpGet.addHeader("X-Requested-By", "ambari");
        httpGet.addHeader(getBasicAuthHeader());
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            HttpResponse messageGot = new HttpResponse(response);
            messageGot.getMessage();

            return messageGot;
        }
    }

    public HttpResponse getHttpResponseViaHead(String connectUrl) throws IOException {
        CloseableHttpClient httpclient = getHttpClient();

        HttpHead httpHead = new HttpHead(connectUrl);
        httpHead.addHeader("Content-Type", "application/json");
        httpHead.addHeader("User-Agent", getUserAgentPerRequest(true));
        httpHead.addHeader("X-Requested-By", "ambari");
        httpHead.addHeader(getBasicAuthHeader());

        // WORKAROUND: https://github.com/Microsoft/azure-tools-for-java/issues/1358
        // The Ambari local account will cause Kerberos authentication initializing infinitely.
        // Set a timer here to cancel the progress.
        httpHead.setConfig(
                RequestConfig
                        .custom()
                        .setSocketTimeout(3 * 1000)
                        .build());

        try (CloseableHttpResponse response = httpclient.execute(httpHead)) {
            HttpResponse messageGot = new HttpResponse(response);
            messageGot.getMessage();

            return messageGot;
        }
    }

    /**
     * To generate a User-Agent for HTTP request with a random UUID.
     *
     * @param isMapToInstallID true for create the relationship between the UUID and InstallationID
     * @return the unique UA string
     */
    private String getUserAgentPerRequest(boolean isMapToInstallID) {
        // String loadingClass = SparkBatchSubmission.class.getClassLoader().getClass().getName().toLowerCase();
        // String requestId = AppInsightsClient.getConfigurationSessionId() == null ?
        //         UUID.randomUUID().toString() :
        //         AppInsightsClient.getConfigurationSessionId();
        //
        // if (isMapToInstallID) {
        //     new AppInsightsHttpRequestInstallIdMapRecord(requestId, getInstallationID()).post();
        // }

        return "Azure Spark Maven plugin";
    }

    @Nullable Header getBasicAuthHeader() {
        Credentials basic = getCredentialsProvider().getCredentials(new AuthScope(AuthScope.ANY));

        if (basic == null) {
            return null;
        }

        String auth = basic.getUserPrincipal().getName() + ":" + basic.getPassword();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
        return new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(encodedAuth));
    }

    /**
     * Get all batches spark jobs.
     *
     * @param connectUrl : eg http://localhost:8998/batches
     * @return response result
     * @throws IOException
     */
    public HttpResponse getAllBatchesSparkJobs(final String connectUrl) throws IOException {
        return getHttpResponseViaGet(connectUrl);
    }

    /**
     * create batch spark job.
     *
     * @param connectUrl          : eg http://localhost:8998/batches
     * @param submissionParameter : spark submission parameter
     * @return response result
     */
    public HttpResponse createBatchSparkJob(final String connectUrl,
                                            final PostBatches submissionParameter) throws IOException {
        CloseableHttpClient httpclient = getHttpClient();
        HttpPost httpPost = new HttpPost(connectUrl);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("User-Agent", getUserAgentPerRequest(true));
        httpPost.addHeader("X-Requested-By", "ambari");
        httpPost.addHeader(getBasicAuthHeader());
        StringEntity postingString = new StringEntity(JsonConverter.of(PostBatches.class).toJson(submissionParameter));
        httpPost.setEntity(postingString);
        try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
            HttpResponse messageGot = new HttpResponse(response);
            messageGot.getMessage();

            return messageGot;
        }
    }

    /**
     * Get batch spark job status.
     *
     * @param connectUrl : eg http://localhost:8998/batches
     * @param batchId    : batch Id
     * @return response result
     * @throws IOException
     */
    public HttpResponse getBatchSparkJobStatus(String connectUrl, int batchId) throws IOException {
        return getHttpResponseViaGet(StringUtils.stripEnd(connectUrl, "/") + "/" + batchId);
    }

    /**
     * Kill batch job.
     *
     * @param connectUrl : eg http://localhost:8998/batches
     * @param batchId    : batch Id
     * @return response result
     * @throws IOException
     */
    public HttpResponse killBatchJob(String connectUrl, int batchId) throws IOException {
        CloseableHttpClient httpclient = getHttpClient();
        HttpDelete httpDelete = new HttpDelete(connectUrl + "/" + batchId);
        httpDelete.addHeader("User-Agent", getUserAgentPerRequest(true));
        httpDelete.addHeader("Content-Type", "application/json");
        httpDelete.addHeader("X-Requested-By", "ambari");
        httpDelete.addHeader(getBasicAuthHeader());

        try (CloseableHttpResponse response = httpclient.execute(httpDelete)) {
            HttpResponse messageGot = new HttpResponse(response);
            messageGot.getMessage();

            return messageGot;
        }
    }

    /**
     * Get batch job full log.
     *
     * @param connectUrl : eg http://localhost:8998/batches
     * @param batchId    : batch Id
     * @return response result
     * @throws IOException
     */
    public HttpResponse getBatchJobFullLog(String connectUrl, int batchId) throws IOException {
        return getHttpResponseViaGet(String.format(
                "%s/%d/log?from=%d&size=%d", connectUrl, batchId, 0, Integer.MAX_VALUE));
    }
}
