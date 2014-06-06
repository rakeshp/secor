/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.writer;

import com.pinterest.secor.common.*;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.ParsedMessage;

import java.io.IOException;

import com.pinterest.secor.util.IdUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message writer appends Kafka messages to local log files.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageWriter {
    private static final Logger LOG = LoggerFactory.getLogger(MessageWriter.class);

    private SecorConfig mConfig;
    private OffsetTracker mOffsetTracker;
    private FileRegistry mFileRegistry;

    public MessageWriter(SecorConfig config, OffsetTracker offsetTracker,
                         FileRegistry fileRegistry) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
    }

    private void adjustOffset(ParsedMessage message) throws IOException {
        TopicPartition topicPartition = new TopicPartition(message.getTopic(),
                                                           message.getKafkaPartition());
        long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
        if (message.getOffset() != lastSeenOffset + 1) {
            // There was a rebalancing event since we read the last message.
            LOG.debug("offset of message " + message +
                      " does not follow sequentially the last seen offset " + lastSeenOffset +
                      ".  Deleting files in topic " + topicPartition.getTopic() + " partition " +
                      topicPartition.getPartition());
            mFileRegistry.deleteTopicPartition(topicPartition);
        }
        mOffsetTracker.setLastSeenOffset(topicPartition, message.getOffset());
    }

    public void write(ParsedMessage message) throws IOException {
        adjustOffset(message);
        TopicPartition topicPartition = new TopicPartition(message.getTopic(),
                                                           message.getKafkaPartition());
        long offset = mOffsetTracker.getAdjustedCommittedOffsetCount(topicPartition);
        String localPrefix = mConfig.getLocalPath() + '/' + IdUtil.getLocalMessageDir();
        LogFilePath path = new LogFilePath(localPrefix, mConfig.getGeneration(), offset, message);
        DataFileWriter writer = mFileRegistry.getOrCreateWriter(path);
        writer.append(createRecord());
        //LOG.debug("appended message " + message + " to file " + path.getLogFilePath() +
        //          ".  File length " + writer.getLength());
    }

	public static GenericRecord createRecord() throws IOException {
		Schema schema = new Schema.Parser().parse(FileRegistry.SCHEMA);


		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);
		return  user1;
	}
}
