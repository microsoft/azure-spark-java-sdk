// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.microsoft.azure.credentials.AzureCliCredentials;
import com.microsoft.azure.credentials.AzureTokenCredentials;

import java.io.IOException;

public class AzureOAuthTokenFetcher implements OAuthTokenFetcher {
    public static final String DEFAULT_MANAGEMENT_RESOURCE = "https://management.azure.com/";
    private final AzureTokenCredentials azureTokenCredentials;
    private final String resource;

    public AzureOAuthTokenFetcher(final AzureTokenCredentials tokenCredentials) {
        this(tokenCredentials, DEFAULT_MANAGEMENT_RESOURCE);
    }

    public AzureOAuthTokenFetcher(final AzureTokenCredentials tokenCredentials, final String resource) {
        this.azureTokenCredentials = tokenCredentials;
        this.resource = resource;
    }

    @Override
    public String get() throws IOException {
        return this.azureTokenCredentials.getToken(resource);
    }

    /**
     * Factory method to build Azure OAuth Token Fetcher from Azure CLI default credentials.
     *
     * @return the instance of {@link AzureOAuthTokenFetcher} based on Azure CLI
     */
    public static AzureOAuthTokenFetcher buildFromAzureCli() {
        AzureCliCredentials cliCredentials;

        try {
            cliCredentials = AzureCliCredentials.create();
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        return new AzureOAuthTokenFetcher(cliCredentials);
    }

    /**
     * Factory method to build Azure OAuth Token Fetcher from Azure CLI default credentials.
     *
     * @param resource the Azure resource to get tokens
     * @return the instance of {@link AzureOAuthTokenFetcher} based on Azure CLI
     */
    public static AzureOAuthTokenFetcher buildFromAzureCli(String resource) {
        AzureCliCredentials cliCredentials;

        try {
            cliCredentials = AzureCliCredentials.create();
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        return new AzureOAuthTokenFetcher(cliCredentials, resource);
    }
}
