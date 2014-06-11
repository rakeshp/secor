package com.pinterest.secor.parser;

import com.pinterest.secor.avro.schema.repository.SchemaRepositoryUtil;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.partition.repository.PartitionRepositoryUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.util.List;

/**
 * Created by rakesh on 03/06/14.
 */
public class WeLogParser extends MessageParser {



	public WeLogParser(SecorConfig config) {
		super(config);
	}

	@Override
	public String[] extractPartitions(Message payload) throws Exception {

		//fetching partitions for the message topic
		List<String> partitionKeys = PartitionRepositoryUtil.getPartitionKeys(payload.getTopic());
		if (partitionKeys != null && partitionKeys.size() > 0) {

			//fetching schema for the message topic
			Schema schema = SchemaRepositoryUtil.getTopicSchema(payload.getTopic());
			if (schema != null) {

				//decoding the message
				BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(payload.getPayload(), null);
				GenericDatumReader<GenericRecord> gdr
						= new GenericDatumReader<GenericRecord>(
						schema);
				GenericRecord record = gdr.read(null, binaryDecoder);

				//building partitions
				String[] partitions = new String[partitionKeys.size()];
				for (int i = 0; i < partitionKeys.size(); i++) {
					String partition = partitionKeys.get(i);
					partitions[i] = partition + "=" + record.get(partition);
				}
				return partitions;
			}

		}

		return new String[0];
	}
}
