package com.pinterest.secor.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;

/**
 * Created by rakesh on 23/06/14.
 */
public class AvroDataFileWriter<D> extends DataFileWriter<D> {

	private long length;
	/**
	 * Construct a writer, not yet open.
	 *
	 * @param dout
	 */
	public AvroDataFileWriter(DatumWriter<D> dout, long length) {
		super(dout);
		this.length = length;
	}

	public void append(D datum, int length) throws IOException {
		super.append(datum);
		this.length += length;
	}

	public long getLength() {
		return length;
	}
}
