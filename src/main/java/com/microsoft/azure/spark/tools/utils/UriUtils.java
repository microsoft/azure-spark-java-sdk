// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import java.net.URI;
import java.net.URISyntaxException;

public class UriUtils {
    public static URI normalizeWithSlashEnding(final URI src) {
        try {
            return src.getPath().endsWith("/")
                    ? src
                    : new URI(
                            src.getScheme(),
                            src.getAuthority(),
                            src.getPath() + "/",
                            src.getQuery(),
                            src.getFragment());
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(x.getMessage(), x);
        }
    }

    public static URI normalizeWithSlashEnding(final String src) {
        return normalizeWithSlashEnding(URI.create(src));
    }
}
