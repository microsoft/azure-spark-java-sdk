// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.legacyhttp;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StreamUtil {

    public static String getResultFromInputStream(InputStream inputStream) throws IOException {
        // change string buffer to string builder for thread-safe
        StringBuilder result = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }

        return result.toString();
    }

    public static HttpResponse getResultFromHttpResponse(CloseableHttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        String reason = response.getStatusLine().getReasonPhrase();
        // Entity for HEAD is empty
        HttpEntity entity = Optional.ofNullable(response.getEntity())
                .orElse(new StringEntity(""));
        try (InputStream inputStream = entity.getContent()) {
            String responseContent = getResultFromInputStream(inputStream);
            return new HttpResponse(code, responseContent, new HashMap<String, List<String>>(), reason);
        }
    }

    public static File getResourceFile(String resource) throws IOException {
        File file = null;
        URL res = streamUtil.getClass().getResource(resource);

        if (res == null) {
            throw new FileNotFoundException("The resource " + resource + " isn't found");
        }

        if (res.toString().startsWith("jar:")) {
            InputStream input = null;
            OutputStream out = null;

            try {
                input = requireNonNull(streamUtil.getClass().getResourceAsStream(resource));
                file = File.createTempFile(String.valueOf(new Date().getTime()), ".tmp");
                out = new FileOutputStream(file);

                int read;
                byte[] bytes = new byte[1024];

                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
            } finally {
                if (input != null) {
                    input.close();
                }

                if (out != null) {
                    out.flush();
                    out.close();
                }

                if (file != null) {
                    file.deleteOnExit();
                }
            }

        } else {
            file = new File(res.getFile());
        }

        return file;
    }

    private static StreamUtil streamUtil = new StreamUtil();
    private static ClassLoader classLoader = requireNonNull(streamUtil.getClass().getClassLoader());
    private static final String SPARK_SUBMISSION_FOLDER = "SparkSubmission";
}
