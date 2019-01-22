/*
 * LazyAsyncValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record;

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A class that lazily fetches and then holds the result of some asynchronous operation on the database. It is useful for
 * situations where we want to repeatedly get the same result from the database while minimizing the number of operations
 * we actually perform. Once the future returned by {@link #get(FDBRecordContext)} completes successfully this class
 * will continue to return a reference to that completed operation. In situations where the get fails for some reason we
 * will clear the reference to the failed operation and re-fetch the value. The asynchronous fetch operation is specified as
 * the <code>loader</code> in the constructor. Since {@link #get(FDBRecordContext)} may only read once from the database,
 * it is critical that the value fetched from FDB never change. For example, it can be used to cache the
 * {@link com.apple.foundationdb.subspace.Subspace} that a fixed
 * {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath} maps to.
 * @param <T> The type of the value contained within the class.
 */
@API(API.Status.EXPERIMENTAL)
public class LazyAsyncValue<T> {
    @Nullable
    private volatile CompletableFuture<T> inner;
    @Nonnull
    private final Function<FDBRecordContext, CompletableFuture<T>> loader;
    private final long deadlineTimeMillis;

    /**
     * Create an instance of this class with the specified <code>loader</code>.
     * @param loader A function that will be invoked to lazily retrieve the value from the database.
     */
    public LazyAsyncValue(@Nonnull Function<FDBRecordContext, CompletableFuture<T>> loader) {
        this(loader, 5000L);
    }

    private LazyAsyncValue(@Nonnull Function<FDBRecordContext, CompletableFuture<T>> loader,
                           long deadlineTimeMillis) {
        this.inner = null;
        this.loader = loader;
        this.deadlineTimeMillis = deadlineTimeMillis;
    }

    /**
     * Get the value wrapped by this object, if the value needs to be fetched (either because it has never been fetched
     * or previous attempts to fetch it have failed) the <code>loader</code> will be called on the provided
     * {@link FDBRecordContext}.
     * @param context The context to use.
     * @return A future that, if it completes successfully, will contain the value fetched.
     */
    public CompletableFuture<T> get(@Nonnull FDBRecordContext context) {
        return (inner == null) ? retrieve(context) : inner;
    }

    private synchronized CompletableFuture<T> retrieve(@Nonnull FDBRecordContext context) {
        if (inner == null) {
            inner = MoreAsyncUtil.getWithDeadline(deadlineTimeMillis, () -> loader.apply(context))
                    .whenComplete((ignored, e) -> {
                        if (e != null) {
                            inner = null;
                        }
                    });
        }
        return inner;
    }

    /**
     * Derive a new {@link LazyAsyncValue} by applying the given transformation.
     * @param transformation The transformation to apply.
     * @param <U> The derived output type.
     * @return The derived {@link LazyAsyncValue}.
     */
    public <U> LazyAsyncValue<U> map(@Nonnull Function<T, U> transformation) {
        return new LazyAsyncValue<>(context -> get(context).thenApply(transformation));
    }
}

