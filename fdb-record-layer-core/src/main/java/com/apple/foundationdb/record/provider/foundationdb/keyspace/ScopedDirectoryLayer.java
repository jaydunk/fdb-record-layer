/*
 * ScopedDirectoryLayer.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.API;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.LazyAsyncValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBReverseDirectoryCache;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Bytes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link LocatableResolver} that uses the FDB directory layer to keep track of the allocation of
 * strings to integers. The {@link KeySpacePath} that it is created with will be the root where the node subspace of the
 * directory layer is located.
 */
@API(API.Status.MAINTAINED)
public class ScopedDirectoryLayer extends LocatableResolver {
    private static final byte[] RESERVED_CONTENT_SUBSPACE_PREFIX = {(byte) 0xFD};
    private static final int STATE_SUBSPACE_KEY_SUFFIX = -10;
    @Nonnull
    private LazyAsyncValue<Subspace> baseSubspace;
    @Nonnull
    private LazyAsyncValue<Subspace> nodeSubspace;
    @Nonnull
    private LazyAsyncValue<Subspace> stateSubspace;
    @Nonnull
    private LazyAsyncValue<DirectoryLayer> directoryLayer;
    @Nonnull
    private final Subspace contentSubspace;

    /**
     * Creates a scoped directory layer. This constructor invokes blocking calls and must not
     * not be called in the context of an asynchronous operation.
     *
     * @param path the path at which the directory layer should live
     * @deprecated use {@link #ScopedDirectoryLayer(FDBRecordContext, KeySpacePath)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    public ScopedDirectoryLayer(@Nonnull KeySpacePath path) {
        this(path.getContext().getDatabase(), path);
    }

    /**
     * Creates a scoped directory layer. This constructor invokes blocking calls and must not
     * not be called in the context of an asynchronous operation.
     *
     * @param context a context that is used only during the construction of this scope in order to
     *   resolve the provided path into a subspace
     * @param path the path at which the directory layer should live
     */
    public ScopedDirectoryLayer(@Nonnull FDBRecordContext context,
                                @Nonnull KeySpacePath path) {
        this(context.getDatabase(), path);
    }

    private ScopedDirectoryLayer(@Nonnull FDBDatabase database,
                                 @Nullable KeySpacePath path) {
        super(database, path);
        if (path == null) {
            this.baseSubspace = new LazyAsyncValue<>(ignore ->
                    CompletableFuture.completedFuture(new Subspace()));
            this.contentSubspace = new Subspace();
        } else {
            this.baseSubspace = new LazyAsyncValue<>(path::toSubspaceAsync);
            this.contentSubspace = new Subspace(RESERVED_CONTENT_SUBSPACE_PREFIX);
        }
        this.nodeSubspace = baseSubspace.map(base ->
                new Subspace(Bytes.concat(base.getKey(), DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey())));
        this.stateSubspace = nodeSubspace.map(node -> node.get(STATE_SUBSPACE_KEY_SUFFIX));
        this.directoryLayer = nodeSubspace.map(node -> new DirectoryLayer(node, contentSubspace));
    }

    /**
     * Creates a default instance of the scoped directory layer. This is a {@link LocatableResolver} that is backed by
     * the default instance of the FDB directory layer.
     * @param database the {@link FDBDatabase} for this resolver
     * @return the global <code>ScopedDirectoryLayer</code> for this database
     */
    public static ScopedDirectoryLayer global(@Nonnull FDBDatabase database) {
        return new ScopedDirectoryLayer(database, null);
    }

    private CompletableFuture<Boolean> exists(@Nonnull FDBRecordContext context, String key) {
        return directoryLayer.get(context)
                .thenCompose(directoryLayer -> directoryLayer.exists(context.ensureActive(), Collections.singletonList(key)));
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        if (metadata != null) {
            throw new IllegalArgumentException("cannot set metadata in ScopedDirectoryLayer");
        }
        return context.instrument(FDBStoreTimer.Events.SCOPED_DIRECTORY_LAYER_CREATE, createInternal(context, key));
    }

    private CompletableFuture<ResolverResult> createInternal(@Nonnull FDBRecordContext context, String key) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        return directoryLayer.get(context)
                .thenCompose(directoryLayer ->
                        directoryLayer.create(context.ensureActive(), Collections.singletonList(key))
                                .thenApply(serializedValue -> deserializeValue(serializedValue.getKey()))
                                .thenCompose(result -> reverseCache.putIfNotExists(context, wrap(key), result.getValue())
                                        .thenApply(ignore -> result)));
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.SCOPED_DIRECTORY_LAYER_READ, readInternal(context, key));
    }

    private CompletableFuture<Optional<ResolverResult>> readInternal(@Nonnull FDBRecordContext context, String key) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        return exists(context, key).thenCompose(keyExists ->
                keyExists ?
                directoryLayer.get(context)
                        .thenCompose(directoryLayer ->
                                directoryLayer.open(context.ensureActive(), Collections.singletonList(key))
                                        .thenApply(serializedValue -> deserializeValue(serializedValue.getKey()))
                                        .thenCompose(result -> reverseCache.putIfNotExists(context, wrap(key), result.getValue()).thenApply(ignore -> result))
                                        .thenApply(Optional::of)) :
                CompletableFuture.completedFuture(Optional.empty()));
    }

    @Override
    protected CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value) {
        FDBReverseDirectoryCache reverseCache = database.getReverseDirectoryCache();
        return reverseCache.get(timer, wrap(value));
    }

    @Override
    public CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value) {
        throw new UnsupportedOperationException("cannot manually add mappings to ScopedDirectoryLayer");
    }

    @Override
    public CompletableFuture<Void> updateMetadata(FDBRecordContext context, String key, byte[] metadata) {
        throw new UnsupportedOperationException("cannot update metadata in ScopedDirectoryLayer");
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        throw new UnsupportedOperationException("cannot manually send allocation window for ScopedDirectoryLayer");
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getMappingSubspace(FDBRecordContext context) {
        return nodeSubspace.get(context).thenApply(nodeSubspace ->
                nodeSubspace.get(nodeSubspace.getKey()).get(0));
    }

    @Override
    protected CompletableFuture<Subspace> getStateSubspace(FDBRecordContext context) {
        return stateSubspace.get(context);
    }

    @Override
    @Nonnull
    public ResolverResult deserializeValue(byte[] value) {
        return new ResolverResult(contentSubspace.unpack(value).getLong(0));
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getBaseSubspace(FDBRecordContext context) {
        return baseSubspace.get(context);
    }

    @VisibleForTesting
    public CompletableFuture<Subspace> getNodeSubspace(FDBRecordContext context) {
        return nodeSubspace.get(context);
    }

    @VisibleForTesting
    public Subspace getContentSubspace() {
        return contentSubspace;
    }

}
