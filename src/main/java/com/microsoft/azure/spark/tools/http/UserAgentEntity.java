// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class UserAgentEntity {
    private final String product;
    private final @Nullable String version;
    private final @Nullable String comment;

    public UserAgentEntity(String product, @Nullable String version, @Nullable String comment) {
        if (StringUtils.isBlank(product)) {
            throw new IllegalArgumentException("Product can't be blank.");
        }

        this.product = product;

        if (StringUtils.isNotBlank(version)) {
            this.version = version;
        } else {
            this.version = null;
        }

        if (StringUtils.isNotBlank(comment)) {
            this.comment = comment;
        } else {
            this.comment = null;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(product);

        if (version != null) {
            builder.append("/");
            builder.append(version);
        }

        if (comment != null) {
            builder.append(" ");
            builder.append("(");
            builder.append(comment);
            builder.append(")");
        }

        return builder.toString();
    }

    public static class Builder {
        private final String product;
        private String version = "";
        private List<String> comments = new ArrayList<>();

        public Builder(String product) {
            this.product = product;
        }

        public Builder version(String ver) {
            this.version = ver;

            return this;
        }

        public Builder comment(String comment) {
            comments.add(comment);

            return this;
        }

        public Builder comment(String key, String value) {
            if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("key and value are not allowed to be blank.");
            }

            return comment(key.trim() + ":" + value.trim());
        }

        public UserAgentEntity build() {
            return new UserAgentEntity(product, version, String.join("; ", comments));
        }
    }
}
