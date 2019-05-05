// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.legacyhttp;

import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.*;

public class SparkBatchSubmissionMock {
    public static SparkBatchSubmission create() throws Exception {
        SparkBatchSubmission submissionMock = mock(SparkBatchSubmission.class);
        ArgumentCaptor<PostBatches> submissionParameterArgumentCaptor;

        submissionParameterArgumentCaptor = ArgumentCaptor.forClass(PostBatches.class);

        when(submissionMock.getBatchSparkJobStatus(anyString(), anyInt())).thenCallRealMethod();
        when(submissionMock.getHttpResponseViaGet(anyString())).thenCallRealMethod();
        when(submissionMock.getHttpClient()).thenCallRealMethod();
        when(submissionMock.getCredentialsProvider()).thenReturn(new BasicCredentialsProvider());
        when(submissionMock.getBasicAuthHeader()).thenCallRealMethod();
        when(submissionMock.createBatchSparkJob(anyString(), submissionParameterArgumentCaptor.capture())).thenCallRealMethod();

        return submissionMock;
    }
}
