package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class IDTuple implements Writable{
	public String id;
	public Integer value;

	public IDTuple() {
	}
	
	public IDTuple(String id,Integer value){
		this.id = id;
		this.value = value;
	}
	
	public IDTuple(String id){
		this.id = id;
		this.value = 1;
	}
	
	public void add(IDTuple tuple){
		value += tuple.value;
	}
	
	public void removeOne(){
		this.value--;
	}
	
	@Override
	public String toString(){
		return id+","+value;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		id = WritableUtils.readString(arg0);
		value = WritableUtils.readVInt(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, id);
		WritableUtils.writeVInt(arg0, value);
		
	}
	
	@Override
	public int hashCode(){
		return id.hashCode();
	}
	
//	public void readFields(DataInput in) throws IOException {
//		symbol = WritableUtils.readString(in);
//		timestamp = in.readLong();
//	}
//
//	@Override
//	public void write(DataOutput out) throws IOException {
//		WritableUtils.writeString(out, symbol);
//		out.writeLong(timestamp);
//	}
}