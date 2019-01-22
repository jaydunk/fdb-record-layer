/*
 * LazyAsyncValueTest.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link LazyAsyncValue}.
 */
@Tag(Tags.RequiresFDB)
public class LazyAsyncValueTest {
    private FDBDatabase database;

    @BeforeEach
    public void setup() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Test
    public void testSupplierExceptionIsNotKept() {
        AtomicInteger count = new AtomicInteger();
        LazyAsyncValue<Integer> value = new LazyAsyncValue<>(context -> {
            count.incrementAndGet();
            return MoreAsyncUtil.delayedFuture(10, TimeUnit.MILLISECONDS).thenRun(() -> {
                throw new RuntimeException("this is testing");
            }).thenApply(vignore -> 1);
        });

        int nTries = 5;
        for (int i = 0; i < nTries; i++) {
            try (FDBRecordContext context = database.openContext()) {
                context.join(value.get(context));
                fail("should get a CompletionException");
            } catch (CompletionException ex) {
                assertThat(ex.getCause(), hasMessageContaining("this is testing"));
            }
        }

        assertEquals(nTries, count.get());
    }

    @Test
    public void testParallelGets() {
        AtomicInteger count = new AtomicInteger();
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("a", KeySpaceDirectory.KeyType.STRING, "a")
                .addSubdirectory(new DirectoryLayerDirectory("b", "b")));
        LazyAsyncValue<Subspace> lazySubspace = new LazyAsyncValue<>(context -> {
            count.incrementAndGet();
            return keySpace.path("a").add("b").toSubspaceAsync(context);
        });

        List<CompletableFuture<Subspace>> futures = IntStream.range(0, 20)
                .mapToObj(i -> database.runAsync(lazySubspace::get))
                .collect(Collectors.toList());
        Set<Subspace> subspaces = new HashSet<>(database.join(AsyncUtil.getAll(futures)));

        Subspace expected = database.join(
                database.runAsync(context -> keySpace.path("a").add("b").toSubspaceAsync(context))
        );

        assertThat("all gets returned the same subspace", subspaces, contains(expected));
        assertThat("we only needed to load the value once", count.get(), equalTo(1));
    }

    @Test
    public void testMap() {
        AtomicInteger count = new AtomicInteger();
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("a", KeySpaceDirectory.KeyType.STRING, "a")
                .addSubdirectory(new DirectoryLayerDirectory("b", "b")));
        LazyAsyncValue<Subspace> lazySubspace = new LazyAsyncValue<>(context -> {
            count.incrementAndGet();
            return keySpace.path("a").add("b").toSubspaceAsync(context);
        });

        List<LazyAsyncValue<Subspace>> derivedLazySubspaces = ImmutableList
                .<LazyAsyncValue<Subspace>>builder()
                .add(lazySubspace.map(subspace -> subspace.get(1)))
                .add(lazySubspace.map(subspace -> subspace.get(2)))
                .add(lazySubspace.map(subspace -> subspace.get(3)))
                .build();

        List<Subspace> derivedSubspaces = database.join(
                AsyncUtil.getAll(
                        derivedLazySubspaces.stream()
                                .map(s -> database.runAsync(s::get))
                                .collect(Collectors.toList())
                )
        );

        Subspace baseSubspace = database.join(
                database.runAsync(context -> keySpace.path("a").add("b").toSubspaceAsync(context))
        );

        List<Subspace> expectedSubspaces = IntStream.range(1, 4)
                .mapToObj(baseSubspace::get)
                .collect(Collectors.toList());

        assertThat(expectedSubspaces, contains(derivedSubspaces.toArray()));
        assertThat("we only need to perform one operation", count.get(), equalTo(1));
    }
}
