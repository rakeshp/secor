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
package com.pinterest.secor.uploader;

import com.pinterest.secor.avro.schema.repository.SchemaRepositoryUtil;
import com.pinterest.secor.common.*;
import com.pinterest.secor.uploader.path.PathGeneratorUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Uploader applies a set of policies to determine if any of the locally stored files should be
 * uploaded to s3.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    private SecorConfig mConfig;
    private OffsetTracker mOffsetTracker;
    private FileRegistry mFileRegistry;
    private ZookeeperConnector mZookeeperConnector;

    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry) {
        this(config, offsetTracker, fileRegistry, new ZookeeperConnector(config));
    }

    // For testing use only.
    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                    ZookeeperConnector zookeeperConnector) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
        mZookeeperConnector = zookeeperConnector;
    }

    private void upload(LogFilePath localPath) throws Exception {
        String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
        LogFilePath s3Path = new LogFilePath(s3Prefix, localPath.getTopic(),
                                             localPath.getPartitions(),
                                             localPath.getGeneration(),
                                             localPath.getKafkaPartition(),
                                             localPath.getOffset(),
                                             localPath.getExtension());
        String localLogFilename = localPath.getLogFilePath();
	    String s3LogFilePath = PathGeneratorUtil.generateUploadPath(s3Path);
        LOG.info("uploading file " + localLogFilename + " to " + s3LogFilePath);
        FileUtil.moveToS3(localLogFilename, s3LogFilePath);
    }

    private void uploadFiles(TopicPartition topicPartition) throws Exception {
        long committedOffsetCount = mOffsetTracker.getTrueCommittedOffsetCount(topicPartition);
        long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
        final String lockPath = "/secor/locks/" + topicPartition.getTopic() + "/" +
                                topicPartition.getPartition();
        // Deleting writers closes their streams flushing all pending data to the disk.
        mFileRegistry.deleteWriters(topicPartition);
        mZookeeperConnector.lock(lockPath);
        try {
            // Check if the committed offset has changed.
            long zookeeperComittedOffsetCount = mZookeeperConnector.getCommittedOffsetCount(
                    topicPartition);
            if (zookeeperComittedOffsetCount == committedOffsetCount) {
                LOG.info("uploading topic " + topicPartition.getTopic() + " partition " +
                         topicPartition.getPartition());
                Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
                for (LogFilePath path : paths) {
                    upload(path);
                }
                mFileRegistry.deleteTopicPartition(topicPartition);
                mZookeeperConnector.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                mOffsetTracker.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
            }
        } finally {
            mZookeeperConnector.unlock(lockPath);
        }
    }

    /**
     * This method is intended to be overwritten in tests.
     */
    protected DataFileReader createReader(LogFilePath logFilePath) throws IOException {

	    DatumReader<GenericRecord> datumReader =
			    new GenericDatumReader<GenericRecord>(SchemaRepositoryUtil.getTopicSchema(logFilePath.getTopic()));
	    return new DataFileReader<GenericRecord>(new File(logFilePath.getLogFilePath()), datumReader);
    }

    private void trim(LogFilePath srcPath, long startOffset) throws Exception {
	    if (startOffset == srcPath.getOffset()) {
		    return;
	    }
	    String srcFilename = srcPath.getLogFilePath();
	    DataFileReader reader = null;
	    DataFileWriter writer = null;
	    LogFilePath dstPath = null;
	    int copiedMessages = 0;
	    // Deleting the writer closes its stream flushing all pending data to the disk.
	    mFileRegistry.deleteWriter(srcPath);
	    try {
		    reader = createReader(srcPath);
		    String codec = null;
		    String extension = "";
		    if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
			    codec = mConfig.getCompressionCodec();
			    extension = "";
		    }
		    while (reader.hasNext()) {
			    GenericRecord value = (GenericRecord) reader.next();
			    Long messageOffset = (Long) value.get(SchemaRepositoryUtil.KAFKA_OFFSET);
			    if (messageOffset != null && messageOffset >= startOffset) {
				    if (writer == null) {
					    String localPrefix = mConfig.getLocalPath() + '/' +
							    IdUtil.getLocalMessageDir();
					    dstPath = new LogFilePath(localPrefix, srcPath.getTopic(),
							    srcPath.getPartitions(), srcPath.getGeneration(),
							    srcPath.getKafkaPartition(), startOffset, extension);
					    writer = mFileRegistry.getOrCreateWriter(dstPath, codec);
				    }
				    writer.append(value);
				    copiedMessages++;
			    }
		    }
	    } finally {
		    if (reader != null) {
			    reader.close();
		    }
	    }
        mFileRegistry.deletePath(srcPath);
        if (dstPath == null) {
            LOG.info("removed file " + srcPath.getLogFilePath());
        } else {
            LOG.info("trimmed " + copiedMessages + " messages from " + srcFilename + " to " +
                    dstPath.getLogFilePath() + " with start offset " + startOffset);
        }
    }

    private void trimFiles(TopicPartition topicPartition, long startOffset) throws Exception {
        Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
        for (LogFilePath path : paths) {
            trim(path, startOffset);
        }
    }

    private void checkTopicPartition(TopicPartition topicPartition) throws Exception {
        final long size = mFileRegistry.getSize(topicPartition);
        final long modificationAgeSec = mFileRegistry.getModificationAgeSec(topicPartition);
        if (size >= mConfig.getMaxFileSizeBytes() ||
                modificationAgeSec >= mConfig.getMaxFileAgeSeconds()) {
            long newOffsetCount = mZookeeperConnector.getCommittedOffsetCount(topicPartition);
            long oldOffsetCount = mOffsetTracker.setCommittedOffsetCount(topicPartition,
                    newOffsetCount);
            long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
            if (oldOffsetCount == newOffsetCount) {
                uploadFiles(topicPartition);
            } else if (newOffsetCount > lastSeenOffset) {  // && oldOffset < newOffset
                LOG.debug("last seen offset " + lastSeenOffset +
                          " is lower than committed offset count " + newOffsetCount +
                          ".  Deleting files in topic " + topicPartition.getTopic() +
                          " partition " + topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset beyond that of the
                // current message.  We need to delete the local file.
                mFileRegistry.deleteTopicPartition(topicPartition);
            } else {  // oldOffsetCount < newOffsetCount <= lastSeenOffset
                LOG.debug("previous committed offset count " + oldOffsetCount +
                          " is lower than committed offset " + newOffsetCount +
                          " is lower than or equal to last seen offset " + lastSeenOffset +
                          ".  Trimming files in topic " + topicPartition.getTopic() +
                          " partition " + topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset lower than that
                // of the current message.  We need to trim local files.
                trimFiles(topicPartition, newOffsetCount);
            }
        }
    }

    public void applyPolicy() throws Exception {
        Collection<TopicPartition> topicPartitions = mFileRegistry.getTopicPartitions();
        for (TopicPartition topicPartition : topicPartitions) {
            checkTopicPartition(topicPartition);
        }
    }
}
