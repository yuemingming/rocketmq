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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static int hashSlotSize = 4;

    private static int indexSize = 20;

    private static int invalidIndex = 0;

    private final int hashSlotNum;

    private final int indexNum;

    private final MappedFile mappedFile;

    private final FileChannel fileChannel;

    private final MappedByteBuffer mappedByteBuffer;

    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset,
                     final long endTimestamp) throws IOException {
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * @param key            消息索引
     * @param phyOffset      消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //如果当前使用条目大于等于允许最大条目数时，则返回 false，表示当前索引文件已写满。如果当前索引文件威胁满则根据 key 算出 key 的 hashcode，然后 keyHash 对 hash 槽取余定位到
        // hashcode 对应的 hash 槽下标，hashcode 对应的 hash 槽的无理地导致为 indexHeader 头部（40 字节) 加上下标乘以每个 hash 槽的大小（4 字节）
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //读取 hash 槽中存储的数据，如果 hash 槽存储的数据小于 0 或大于当前索引文件中索引的条目格式，则将 slotValue 设置为 0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //计算带存储消息的时间戳与第一条消息时间戳的差值，并转换成秒¬
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                //将条目信息存储在 indexFile 中
                //1.计算添加条目的其实物理偏移量，等于头部字节长度 + hash 槽数量 * 单个 hash 槽大小（4 个字节）+当前 Index 条目个数 * 单个 Index 条目大小（20 个字节）
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize +
                        this.indexHeader.getIndexCount() * indexSize;
                //依次将 hashcode/消息物理偏移量、消息存储时间戳、索引文件时间戳、当前 Hash 槽的值存入 MappedByteBuffer 中。
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //将当前 Index 中包含的条目数量存入 Hash 槽中，将覆盖原先的 Hash 槽的值
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                //更新文件索引头信息。如果当前文件只包含一个条目，更新 beginPhyOffset 与 beginTimeStamp,更新 endPhyOffset/endTimeStamp 当前文件使用索引条目等信息。
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount() +
                    "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result ||
                (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key        索引 key
     * @param maxNum     本次查找的最大消息条数
     * @param begin      开始时间戳
     * @param end        结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin,
                                final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            //根据 key 计算出 hashcode，然后 keyHash 对 hash 槽数量取余定位到 hashcode对应的 hash 槽下标，hashcode 对应的物理地址为 Indexheader
            // (20字节)加上下标乘以每个 hash 槽的大小（4 字节）
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }
                //如果槽位置无效，则直接返回
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount() ||
                        this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        //读取数据，直到到达链表尾部或者达到最大查询条目。
                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize +
                                nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount() ||
                                prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
