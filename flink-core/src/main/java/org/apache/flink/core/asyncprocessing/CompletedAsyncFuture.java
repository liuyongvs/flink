/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

/** A {@link AsyncFuture} that has already been completed when it is created. */
@Internal
public class CompletedAsyncFuture<T> implements InternalAsyncFuture<T> {

    T result;

    // no public access
    public CompletedAsyncFuture(T result) {
        this.result = result;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() {
        return result;
    }

    @Override
    public <U> InternalAsyncFuture<U> thenApply(
            FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
        return InternalAsyncFutureUtils.completedFuture(
                FunctionWithException.unchecked(fn).apply(result));
    }

    @Override
    public InternalAsyncFuture<Void> thenAccept(
            ThrowingConsumer<? super T, ? extends Exception> action) {
        ThrowingConsumer.unchecked(action).accept(result);
        return InternalAsyncFutureUtils.completedVoidFuture();
    }

    @Override
    public <U> InternalAsyncFuture<U> thenCompose(
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    action) {
        return (InternalAsyncFuture<U>) FunctionWithException.unchecked(action).apply(result);
    }

    @Override
    public <U, V> InternalAsyncFuture<V> thenCombine(
            StateFuture<? extends U> other,
            BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception> fn) {
        return (InternalAsyncFuture<V>)
                other.thenCompose(
                        (u) -> {
                            V v = fn.apply(result, u);
                            return (InternalAsyncFuture<V>)
                                    InternalAsyncFutureUtils.completedFuture(v);
                        });
    }

    @Override
    public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
            FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        Object r =
                test
                        ? FunctionWithException.unchecked(actionIfTrue).apply(result)
                        : FunctionWithException.unchecked(actionIfFalse).apply(result);
        return InternalAsyncFutureUtils.completedFuture(Tuple2.of(test, r));
    }

    @Override
    public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        U r = test ? FunctionWithException.unchecked(actionIfTrue).apply(result) : null;
        return InternalAsyncFutureUtils.completedFuture(Tuple2.of(test, r));
    }

    @Override
    public InternalAsyncFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
            ThrowingConsumer<? super T, ? extends Exception> actionIfFalse) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        if (test) {
            ThrowingConsumer.unchecked(actionIfTrue).accept(result);
        } else {
            ThrowingConsumer.unchecked(actionIfFalse).accept(result);
        }
        return InternalAsyncFutureUtils.completedFuture(test);
    }

    @Override
    public InternalAsyncFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        if (test) {
            ThrowingConsumer.unchecked(actionIfTrue).accept(result);
        }
        return InternalAsyncFutureUtils.completedFuture(test);
    }

    @Override
    public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue,
            FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                    actionIfFalse) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        InternalAsyncFuture<?> actionResult;
        if (test) {
            actionResult =
                    (InternalAsyncFuture)
                            FunctionWithException.unchecked(actionIfTrue).apply(result);
        } else {
            actionResult =
                    (InternalAsyncFuture)
                            FunctionWithException.unchecked(actionIfFalse).apply(result);
        }
        return actionResult.thenApply((e) -> Tuple2.of(test, e));
    }

    @Override
    public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue) {
        boolean test = FunctionWithException.unchecked(condition).apply(result);
        if (test) {
            StateFuture<U> actionResult =
                    FunctionWithException.unchecked(actionIfTrue).apply(result);
            return (InternalAsyncFuture<Tuple2<Boolean, U>>)
                    actionResult.thenApply((e) -> Tuple2.of(true, e));
        } else {
            return InternalAsyncFutureUtils.completedFuture(Tuple2.of(false, null));
        }
    }

    @Override
    public void complete(T result) {
        throw new UnsupportedOperationException("This state future has already been completed.");
    }

    @Override
    public void completeExceptionally(String message, Throwable ex) {
        throw new UnsupportedOperationException("This state future has already been completed.");
    }

    @Override
    public void thenSyncAccept(ThrowingConsumer<? super T, ? extends Exception> action) {
        ThrowingConsumer.unchecked(action).accept(result);
    }
}
