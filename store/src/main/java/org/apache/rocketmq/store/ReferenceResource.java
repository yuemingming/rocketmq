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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    protected final AtomicLong refCount = new AtomicLong(1);

    protected volatile boolean available = true;

    protected volatile boolean cleanupOver = false;

    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 初次调用是 this.available 为 true，设置 available 为 false，并设置初次关闭的时间戳为当前时间戳，然后调用 release()方法尝试释放资源，release 只有在引用次数小于 1
     * 的情况下才会释放资源；如果引用次数大于 0，对比当前时间与 firstShutdownTimeStamp,如果已经超过了器最大拒绝存活期，每执行一次，将引用数减少 1000，知道引用数小于 0 时通过执行 release
     * 方法释放资源
     *
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        //将引用次数建议，如果引用次数小于等于 0，则执行 cleanup 方法。
        long value = this.refCount.decrementAndGet();
        if (value > 0) {
            return;
        }

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断是否清理完成，判断标准是引用次数小于等于 0 并且 cleanupOver 为 true，cleanupOver 为 true 的触发条件是 release 成功将 Map碰到By特Buffer 资源释放。
     *
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
