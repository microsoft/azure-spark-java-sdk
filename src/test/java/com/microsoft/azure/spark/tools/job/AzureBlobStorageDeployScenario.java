// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.mockito.Mockito;
import rx.Observable;

import com.microsoft.azure.spark.tools.utils.WasbUri;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.core.Base64;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.withSettings;

public class AzureBlobStorageDeployScenario {
    private AzureBlobStorageDeploy deploy;
    private Observable<String> deploySub;
    private CloudStorageAccount mockStorageAccount;
    private CloudBlobClient mockBlobClient;
    private CloudBlobContainer mockBlobContainer;
    private CloudBlockBlob mockBlob;

    static class DummyFileInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            return 0;
        }
    }

    static class DummyDeploy implements Deployable {
        @Override
        public Observable<String> deploy(File src) {
            return Observable.empty();
        }
    }

    @Before
    public void Setup() {

    }

    @Given("create an AzureBlobStorageDeploy with access key {string} and fs root {string}")
    public void createAzureBlobStorageDeploy(String key, String fsRoot) {
        this.deploy = new AzureBlobStorageDeploy(Base64.encode(key.getBytes()), WasbUri.parse(fsRoot));
    }

    @Then("check the AzureBlobStorageDeploy storage account credential is {string} and account is {string}")
    public void checkTheAzureBlobStorageDeployStorageAccountCredential(String expectKey, String expectAccount) {
        StorageCredentialsAccountAndKey actual = (StorageCredentialsAccountAndKey)
                this.deploy.getStorageAccount().getCredentials();

        assertEquals(expectKey, new String(actual.exportKey()));
        assertEquals(expectAccount, actual.getAccountName());
    }

    @Given("create an AzureBlobStorageDeploy with mocked storage account and fs root {string}")
    public void createAnAzureBlobStorageDeployWithMockedStorageAccount(String fsRoot) throws Throwable {
        // Mock storage account
        this.mockStorageAccount = Mockito.mock(CloudStorageAccount.class);
        this.mockBlobClient = Mockito.mock(CloudBlobClient.class);
        this.mockBlobContainer = Mockito.mock(CloudBlobContainer.class);
        this.mockBlob = Mockito.mock(CloudBlockBlob.class);

        WasbUri fsUri = WasbUri.parse(fsRoot);
        this.deploy = Mockito.mock(
                AzureBlobStorageDeploy.class,
                withSettings()
                        .useConstructor(this.mockStorageAccount, fsUri)
                        .defaultAnswer(CALLS_REAL_METHODS));

        doReturn(mockBlobClient).when(mockStorageAccount).createCloudBlobClient();
        doReturn(mockBlobContainer).when(mockBlobClient).getContainerReference(fsUri.getContainer());
        doNothing().when(mockBlobContainer).create(BlobContainerPublicAccessType.BLOB, null, null);
    }

    @And("perform an Azure blob deploy operation for the local file {string}")
    public void performAzureBlobDeploy(String mockFilePath) throws Throwable {
        File mockFile = new File(mockFilePath);

        doReturn(this.mockBlob).when(this.mockBlobContainer)
                .getBlockBlobReference(argThat(someString -> someString.endsWith(mockFile.getName())));

        doReturn(this.deploy.getFsRoot().getUri().resolve(
                        new DummyDeploy().getDestRelativePath() + mockFile.getName())).when(this.mockBlob)
                .getUri();

        doReturn(new DummyFileInputStream()).when(this.deploy).createFileInputStream(mockFile);

        this.deploySub = this.deploy.deploy(mockFile);
    }

    @Then("check the destination URI should match regex {string}")
    public void checkTheDestinationURIRegexMatch(String expectRegex) {
        String actualDestUri = this.deploySub.toBlocking().single();

        assertTrue("The actual deploy destination URI " + actualDestUri + " doesn't match " + expectRegex,
                Pattern.compile(expectRegex).matcher(actualDestUri).matches());

    }
}
