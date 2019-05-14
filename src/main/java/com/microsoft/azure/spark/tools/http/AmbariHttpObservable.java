// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.microsoft.azure.spark.tools.utils.Lazy;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public class AmbariHttpObservable extends HttpObservable {
    private Lazy<HeaderGroup> ambariDefaultHeaderGroup = new Lazy<>();

    public AmbariHttpObservable() {
        this(null, null);
    }

    public AmbariHttpObservable(final @Nullable String username, final @Nullable String password) {
        super(username, password);
    }

    @Override
    public HeaderGroup getDefaultHeaderGroup() {
        return ambariDefaultHeaderGroup.getOrEvaluate(() -> {
            HeaderGroup hg = new HeaderGroup();

            Arrays.stream(super.getDefaultHeaderGroup().getAllHeaders())
                    .filter(header -> !(StringUtils.equalsIgnoreCase(header.getName(), "X-Requested-By")
                            && StringUtils.equalsIgnoreCase(header.getValue(), "ambari")))
                    .forEach(hg::addHeader);

            hg.addHeader(new BasicHeader("X-Requested-By", "ambari"));

            return hg;
        });
    }
}
