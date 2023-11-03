/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.common;

public interface IQueueHandler<T> {
    enum ResultStatus {
        /** Item was successfully processed */
        HANDLED,

        /** Item was not processed, and the consumer should shutdown */
        NOT_HANDLED_SHUTDOWN,

        /** Item processing failed, and the item should be retried immediately */
        FAILED_RETRY,

        /** Item processing failed, and the item should not be retried (unsuccessfully processed) */
        FAILED_NO_RETRY,

        /** Item processing failed, and the item should be re-queued */
        FAILED_RESUBMIT
    }

    class Result<T> {
        private final ResultStatus _status;
        private final Throwable _throwable;
        private final T _newItem;

        public Result(final ResultStatus status) {
            _status = status;
            _throwable = null;
            _newItem = null;
        }

        public Result(final ResultStatus status, final Throwable throwable) {
            _status = status;
            _throwable = throwable;
            _newItem = null;
        }

        public Result(final ResultStatus status, final Throwable throwable, final T newItem) {
            _status = status;
            _throwable = throwable;
            _newItem = newItem;
        }

        public ResultStatus status() {
            return _status;
        }

        public Throwable throwable() {
            return _throwable;
        }

        public T newItem() {
            return _newItem;
        }
    }

    Result<T> handleItem(T item);
}
