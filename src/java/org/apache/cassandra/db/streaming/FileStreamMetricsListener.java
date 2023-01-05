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

package org.apache.cassandra.db.streaming;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counter;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Used to keep track of the bytes streamed for a given file as bytes of the file are transferred and report them to StreamingMetrics.
 */
public class FileStreamMetricsListener
{
    public static final FileStreamMetricsListener NO_OP = new FileStreamMetricsListener(0L, FBUtilities.getLocalAddressAndPort(), ProgressInfo.Direction.IN){
        @Override
        public void onStreamingBytesTransferred(long cumulativeBytesTransferred)
        {
            //no-op
        }
    };

    private final long totalSize;
    private final Counter totalOutgoingBytesStreamed;
    private final Counter peerOutgoingBytesStreamed;
    private long lastSeenBytes = 0L;

    public FileStreamMetricsListener(long totalSize, InetAddressAndPort peer, ProgressInfo.Direction direction)
    {
        this.totalSize = totalSize;
        if (direction == ProgressInfo.Direction.OUT)
        {
            this.totalOutgoingBytesStreamed = StreamingMetrics.totalOutgoingBytes;
            this.peerOutgoingBytesStreamed = StreamingMetrics.get(peer).outgoingBytes;
        }
        else
        {
            this.totalOutgoingBytesStreamed = StreamingMetrics.totalIncomingBytes;
            this.peerOutgoingBytesStreamed = StreamingMetrics.get(peer).incomingBytes;
        }

    }

   @VisibleForTesting
   public FileStreamMetricsListener(long totalSize, Counter totalOutgoingBytesStreamed, Counter peerOutgoingBytesStreamed)
    {
        this.totalSize = totalSize;
        this.totalOutgoingBytesStreamed = totalOutgoingBytesStreamed;
        this.peerOutgoingBytesStreamed = peerOutgoingBytesStreamed;
    }

    /**
     * Report to the FileStreamMetricsHandler the total number of bytes transferred so far.
     * @param cumulativeBytesTransferred The total number of bytes transferred for a file so far. We use this and calculate
     *                                   the delta from the last call ourselves as this is the number which the Stream Reader
     *                                   and Writer classes track
     */
    public void onStreamingBytesTransferred(long cumulativeBytesTransferred)
    {
        long delta = cumulativeBytesTransferred - lastSeenBytes;
        assert delta >= 0;
        this.totalOutgoingBytesStreamed.inc(delta);
        this.peerOutgoingBytesStreamed.inc(delta);
        lastSeenBytes = cumulativeBytesTransferred;
    }

    /**
     * onStreamSuccessful lets the caller of the FileStreamMetricsHandler ensure that the bytes reported from the
     * FileStreaMetricsHandler is equal to the declared size of the file that was going to be transferred at the creation
     * of the FileStreamMetricsHandler.
     */
    public void onStreamSuccessful()
    {
        assert lastSeenBytes == totalSize;
    }

}
