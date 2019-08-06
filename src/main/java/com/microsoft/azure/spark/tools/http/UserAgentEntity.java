// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A User Agent Entity class to help generate a formatted UA string in HTTP request header.
 * The format would be like: product/version (comment)
 * such as: spark-java-sdk/0.1.0 (os:Linux; jdk:1.8)
 *
 * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent">Mozilla User-Agent Syntax</a>
 */
public class UserAgentEntity {
    private final String product;
    private final @Nullable String version;
    private final @Nullable String comment;

    /**
     * Constructor of User Agent Entity.
     *
     * @param product the product name in User Agent
     * @param version the product version
     * @param comment the comment for the product
     */
    public UserAgentEntity(final String product,
                           final @Nullable String version,
                           final @Nullable String comment) {
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

        public Builder(final String product) {
            this.product = product;
        }

        /**
         * Set version option.
         *
         * @param ver the version string to set
         * @return the {@link Builder} instance for fluent invoking
         */
        public Builder version(final String ver) {
            this.version = ver;

            return this;
        }

        /**
         * Set comment option.
         *
         * @param comment the comment string to set
         * @return the {@link Builder} instance for fluent invoking
         */
        public Builder comment(final String comment) {
            comments.add(comment);

            return this;
        }

        /**
         * Set comment option with key and value.
         *
         * @param key the key
         * @param value the value
         * @return the {@link Builder} instance for fluent invoking
         */
        public Builder comment(final String key, final String value) {
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
