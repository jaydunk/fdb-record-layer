/*
 * ScopedInterningLayer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.LazyAsyncValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link LocatableResolver} that is backed by the {@link StringInterningLayer}.
 */
@API(API.Status.MAINTAINED)
public class ScopedInterningLayer extends LocatableResolver {
    private static final byte[] GLOBAL_SCOPE_PREFIX_BYTES = new byte[]{(byte) 0xFC};
    private static final int STATE_SUBSPACE_KEY_SUFFIX = -10;

    @Nonnull
    private LazyAsyncValue<Subspace> baseSubspace;
    @Nonnull
    private LazyAsyncValue<Subspace> nodeSubspace;
    @Nonnull
    private LazyAsyncValue<Subspace> stateSubspace;
    @Nonnull
    private LazyAsyncValue<StringInterningLayer> interningLayer;

    /**
     * Creates a resolver rooted at the provided <code>KeySpacePath</code>.
     *
     * @param path the {@link KeySpacePath} where this resolver is rooted
     * @deprecated use {@link #ScopedInterningLayer(FDBRecordContext, KeySpacePath)} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public ScopedInterningLayer(@Nonnull KeySpacePath path) {
        this(path.getContext(), path);
    }

    /**
     * Creates a resolver rooted at the provided <code>KeySpacePath</code>.
     * @param context context used to resolve the provided <code>path</code>. This context
     *   is only used during the construction of this class and at no other point
     * @param path the {@link KeySpacePath} where this resolver is rooted
     */
    public ScopedInterningLayer(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
        this(context.getDatabase(), path);
    }

    private ScopedInterningLayer(@Nonnull FDBDatabase database,
                                 @Nullable KeySpacePath path) {
        super(database, path);
        boolean isRootLevel;
        if (path == null) {
            isRootLevel = true;
            this.baseSubspace = new LazyAsyncValue<>(ignore ->
                    CompletableFuture.completedFuture(new Subspace()));
            this.nodeSubspace = new LazyAsyncValue<>(ignore ->
                    CompletableFuture.completedFuture(new Subspace(GLOBAL_SCOPE_PREFIX_BYTES)));
        } else {
            isRootLevel = false;
            this.baseSubspace = new LazyAsyncValue<>(path::toSubspaceAsync);
            this.nodeSubspace = baseSubspace;
        }
        this.stateSubspace = nodeSubspace.map(node -> node.get(STATE_SUBSPACE_KEY_SUFFIX));
        this.interningLayer = nodeSubspace.map(node -> new StringInterningLayer(node, isRootLevel));
    }

    /**
     * Creates a default instance of the scoped interning layer.
     * @param database the {@link FDBDatabase} for this resolver
     * @return the global <code>ScopedInterningLayer</code> for this database
     */
    public static ScopedInterningLayer global(@Nonnull FDBDatabase database) {
        return new ScopedInterningLayer(database, null);
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_READ,
                interningLayer.get(context).thenCompose(layer -> layer.read(context, key)));
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context,
                                                       @Nonnull String key,
                                                       @Nullable byte[] metadata) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_CREATE,
                interningLayer.get(context).thenCompose(layer -> layer.create(context, key, metadata)));
    }

    @Override
    protected CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value) {
        FDBRecordContext context = database.openContext();
        context.setTimer(timer);
        return interningLayer.get(context)
                .thenCompose(layer -> layer.readReverse(context, value))
                .whenComplete((ignored, th) -> context.close());
    }

    @Override
    public CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value) {
        return interningLayer.get(context)
                .thenCompose(layer -> layer.setMapping(context, key, value));
    }

    @Override
    public CompletableFuture<Void> updateMetadata(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        return interningLayer.get(context)
                .thenCompose(layer -> layer.updateMetadata(context, key, metadata));
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        return database.runAsync(context -> interningLayer.get(context)
                .thenCompose(layer -> layer.setWindow(context, count)));
    }

    @Override
    protected CompletableFuture<Subspace> getStateSubspace(FDBRecordContext context) {
        return stateSubspace.get(context);
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getMappingSubspace(FDBRecordContext context) {
        return interningLayer.get(context).thenApply(StringInterningLayer::getMappingSubspace);
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getBaseSubspace(FDBRecordContext context) {
        return baseSubspace.get(context);
    }

    @Override
    @Nonnull
    public ResolverResult deserializeValue(byte[] value) {
        return StringInterningLayer.deserializeValue(value);
    }

}
