package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class IntegerWritable implements WritableComparable<IntegerWritable> {

	public Integer value;
	
	/**
	 * Constructor.
	 */
	public IntegerWritable() { }
	
	/**
	 * Constructor.
	 * @param key Stock symbol. i.e. APPL
	 */
	public IntegerWritable(Integer key) {
		this.value = key;
	}
	
	@Override
	public String toString() {
		return value.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		value = WritableUtils.readVInt(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, value);
	}

	@Override
	public int compareTo(IntegerWritable o) {
		return -value.compareTo(o.value);
	}
}