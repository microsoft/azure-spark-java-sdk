// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.utils.WasbUri;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import rx.Observable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class AzureBlobStorageDeploy implements Deployable, Logger {
    private final CloudStorageAccount storageAccount;
    private final WasbUri fsRoot;

    public AzureBlobStorageDeploy(final CloudStorageAccount storageAccount, final WasbUri fsRoot) {
        this.storageAccount = storageAccount;
        this.fsRoot = fsRoot;
    }

    /**
     * Constructor of AzureBlobStorageDeploy.
     *
     * @param storageAccessKey the Azure Blob storage access key
     * @param fsRoot the WASB URI for Blob root with container
     */
    public AzureBlobStorageDeploy(final String storageAccessKey, final WasbUri fsRoot) {
        this.fsRoot = fsRoot;

        StorageCredentialsAccountAndKey storageCredentials =
                new StorageCredentialsAccountAndKey(fsRoot.getStorageAccount(), storageAccessKey);
        try {
            this.storageAccount = new CloudStorageAccount(storageCredentials, true);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public CloudStorageAccount getStorageAccount() {
        return storageAccount;
    }

    public WasbUri getFsRoot() {
        return fsRoot;
    }

    /**
     * Upload a local file to Azure Blob storage.
     *
     * @param fileToUpload the local file to upload
     * @param blobName the blob name to upload
     * @return the WASB URI for the file uploaded
     * @throws URISyntaxException the wrong blob or container name caused URI syntax exception
     * @throws StorageException the Azure storage exception when operating blob containers
     * @throws IOException the networking or local storage exceptions
     */
    private String uploadFileAsBlob(final File fileToUpload, final String blobName)
            throws URISyntaxException, StorageException, IOException {
        final CloudBlobContainer blobContainer = getBlobContainer();
        blobContainer.createIfNotExists(BlobContainerPublicAccessType.BLOB, null, null);

        final CloudBlob blob = blobContainer.getBlockBlobReference(blobName);
        blob.upload(createFileInputStream(fileToUpload), fileToUpload.length());
        return WasbUri.parse(blob.getUri().toString()).getUri().toString();
    }

    private CloudBlobContainer getBlobContainer() throws URISyntaxException, StorageException {
        final CloudBlobClient blobClient = getStorageAccount().createCloudBlobClient();
        return blobClient.getContainerReference(getFsRoot().getContainer());
    }

    protected InputStream createFileInputStream(final File file) throws FileNotFoundException {
        return new FileInputStream(file);
    }

    @Override
    public Observable<String> deploy(final File src) {
        final String destRelatedBlobName = getDestRelativePath() + src.getName();

        return Observable.fromCallable(() ->
                getFsRoot().getUri().resolve(uploadFileAsBlob(src, destRelatedBlobName)).toString());
    }
}
