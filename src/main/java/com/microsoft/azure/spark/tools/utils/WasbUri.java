// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.UnknownFormatConversionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class WasbUri {
    public static final Pattern WASB_URI_PATTERN = Pattern.compile(
            "^wasb[s]?://(?<container>[^/.]+)@(?<storageAccount>[^/.]+)\\.blob\\.(?<endpointSuffix>[^/]+)"
                    + "(:(?<port>[0-9]+))?(/(?<path>.*))?$",
            Pattern.CASE_INSENSITIVE);
    public static final Pattern HTTP_URI_PATTERN = Pattern.compile(
            "^http[s]?://(?<storageAccount>[^/.]+)\\.blob\\.(?<endpointSuffix>[^/]+)/(?<container>[^/.]+)"
                    + "(:(?<port>[0-9]+))?(/(?<path>.*))?$",
            Pattern.CASE_INSENSITIVE);

    private final URI rawUri;
    private final LaterInit<String> container = new LaterInit<>();
    private final LaterInit<String> storageAccount = new LaterInit<>();
    private final LaterInit<String> endpointSuffix = new LaterInit<>();
    private final LaterInit<String> absolutePath = new LaterInit<>();

    private WasbUri(final URI rawUri) {
        this.rawUri = rawUri;
    }

    public URI getRawUri() {
        return rawUri;
    }

    public URI getUri() {
        return URI.create(String.format("wasbs://%s@%s.blob.%s%s",
                getContainer(), getStorageAccount(), getEndpointSuffix(), getAbsolutePath()));
    }

    public URL getUrl() {
        try {
            return URI.create(String.format("https://%s.blob.%s/%s%s",
                    getStorageAccount(), getContainer(), getEndpointSuffix(), getAbsolutePath())).toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public String getContainer() {
        return this.container.get();
    }

    public String getStorageAccount() {
        return this.storageAccount.get();
    }

    public String getEndpointSuffix() {
        return this.endpointSuffix.get();
    }

    public String getAbsolutePath() {
        return this.absolutePath.get();
    }

    @Override
    public String toString() {
        return rawUri.toString();
    }

    public static WasbUri parse(final String blobUri) {
        Matcher matcher;
        if (StringUtils.startsWithIgnoreCase(blobUri, "wasb")) {
            matcher = WASB_URI_PATTERN.matcher(blobUri);
        } else if (StringUtils.startsWithIgnoreCase(blobUri, "http")) {
            matcher = HTTP_URI_PATTERN.matcher(blobUri);
        } else {
            throw new UnknownFormatConversionException("Unsupported Azure blob URI Scheme: " + blobUri);
        }

        if (matcher.matches()) {
            final WasbUri wasbUri = new WasbUri(URI.create(blobUri));

            wasbUri.container.set(matcher.group("container"));
            wasbUri.storageAccount.set(matcher.group("storageAccount"));
            wasbUri.endpointSuffix.set(matcher.group("endpointSuffix"));

            final String pathMatched = matcher.group("path");
            final String absolutePathMatched = URI.create("/" + (pathMatched == null ? "" : pathMatched)).getPath();
            wasbUri.absolutePath.set(absolutePathMatched);

            return wasbUri;
        }

        throw new UnknownFormatConversionException("Unmatched Azure blob URI: " + blobUri);
    }

    public static boolean isType(final String uri) {
        return WASB_URI_PATTERN.matcher(uri).matches() || HTTP_URI_PATTERN.matcher(uri).matches();
    }
}
