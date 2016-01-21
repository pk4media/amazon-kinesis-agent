/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

/**
 * Returns one record per N lines.
 * TODO: Limitation of this splitter is that it requires a newline at the end
 *       of the file, otherwise it will miss the last record. We should be able
 *       to handle this at the level of the {@link IParser} implementation.
 * TODO: Should we parametrize the line delimiter?
 */
public class MultiLineSplitter implements ISplitter {
    public final int maxLinesPerRecord;

    public MultiLineSplitter(int maxLinesPerRecord) {
        Preconditions.checkArgument(maxLinesPerRecord >= 2);
        this.maxLinesPerRecord = maxLinesPerRecord;
    }

    @Override
    public int locateNextRecord(ByteBuffer buffer) {
        return advanceBufferToNextExtent(buffer);
    }

    /**
     * Advances the buffer current position to be at the index at the beginning of the
     * next line after the maximum number of lines have been reached, or else the end
     * of the buffer if there are not enough lines to reach the maximum.
     *
     * @return {@code position} of the buffer at the starting index of the next set of lines;
     *         {@code -1} if the end of the buffer was reached.
     */
    private int advanceBufferToNextExtent(ByteBuffer buffer) {
        // number of lines we've retrieved from the buffer
        int matchedLines = 0;
        boolean isNewLine = false;
        while (buffer.hasRemaining()) {
            // start matching from the current position on a line by line basis
            if (buffer.get() == SingleLineSplitter.LINE_DELIMITER) {
                isNewLine = true;
                matchedLines += 1;
                if (matchedLines >= maxLinesPerRecord) {
                  return buffer.position();
                }
            } else {
              // For some reason we didn't end on a new line
              isNewLine = false;
            }
        }

        // as long as we ended on a full line, complete the record even if we
        // didn't hit the maximum number of lines
        if (isNewLine) {
          return buffer.position();
        }

        return -1;
    }
}
