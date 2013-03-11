package pagerank;


//import java.io.ByteArrayInputStream;

//import java.io.ByteArrayOutputStream;
import java.io.DataInput;
//import java.io.DataInputStream;
import java.io.DataOutput;
//import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;


//import org.apache.hadoop.io.Writable;


import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.map.HMapIFW;

import pagerank.PageRankNode;
import pagerank.PageRankNode.Type;

/**
 * Representation of a graph node for PageRank with specific source node. 
 *
 * @author Sikai Qu
 */


public class PageRankNodeMultiSrc extends PageRankNode {
	
	//private ArrayListOfIntsWritable srcnode;
	private HMapIFW srcnode;

	
	public PageRankNodeMultiSrc()
	{
		srcnode=new HMapIFW();
	}
	
	public void setSrcNode(int[] list) {
		for(int i=0;i<list.length;i++)
		{			
			this.srcnode.put(list[i],0.0f);
		}
	}
	
	public void setPageRank(HMapIFW list) {
		this.srcnode = list;
	}
	
	public void setSrcNodePG(int key,float value)
	{
		srcnode.put(key, value);
	}
	
	public int[] getSrc()
	{
		Set<Integer> a =srcnode.keySet();
		int size = srcnode.size();
		int[] keys =new int [size];
		int i=0;
		Iterator<Integer> values = a.iterator();
		while(values.hasNext())
		{
			keys[i]=values.next();
			i++;
		}
		return keys;
	}
	
	public float getPageRank(int key)
	{
		return srcnode.get(key);
	}
	
	
	public HMapIFW getPageRankM()
	{
		return srcnode;
	}
	
	public void Sum(HMapIFW a)
	{
		/*int[] src=a.getKeys();
		for (int i=0; i<a.length();i++)
		{
			if(this.srcnode.containsKey(src[i]))
			{
				this.srcnode.put(src[i], this.getPageRank(src[i])+a.get(src[i]));
			}
			else{
				this.srcnode.put(src[i], a.get(src[i]));
			}
		}*/
		this.srcnode.plus(a);
	}
	
	
	public void readFields(DataInput in) throws IOException
	{
		
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();

		if (type.equals(Type.Mass)) {
			pagerank = in.readFloat();
			srcnode = new HMapIFW();
			srcnode.readFields(in);
			return;
		}

		if (type.equals(Type.Complete)) {
			pagerank = in.readFloat();
			adjacenyList = new ArrayListOfIntsWritable();
			adjacenyList.readFields(in);
			srcnode = new HMapIFW();
			srcnode.readFields(in);
			return;
		}
		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		
		out.writeByte(type.val);
		out.writeInt(nodeid);

		if (type.equals(Type.Mass)) {
			out.writeFloat(pagerank);
			srcnode.write(out);
			return;
		}

		if (type.equals(Type.Complete)) {
			out.writeFloat(pagerank);
			adjacenyList.write(out);
			srcnode.write(out);
			return;
		}

		adjacenyList.write(out);
		
	}
	
	@Override
	public String toString() {
		String str = super.toString()+ " "+ srcnode.toString();
		return str;
	}

}
