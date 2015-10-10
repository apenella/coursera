import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Compute shortest paths from a given source.
 */
public class ShortestPathsComputation extends BasicComputation<
    IntWritable, IntWritable, NullWritable, IntWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
          "The shortest paths id");

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<IntWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute( Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {

	if (getSuperstep() == 0) {
    		// initially, no node is reachable
    		vertex.setValue(new IntWritable(Integer.MAX_VALUE));
  	}
  
  	Integer minDist = isSource(vertex) ? new Integer(0) : Integer.MAX_VALUE;
  	// check the incoming messages 
  	// and collect the minimal distance
  	// (for the source this will be always 0)
  	for (IntWritable message : messages) {
    		minDist = Math.min(minDist, message.get());
  	}
  
  	// check if the new minimal distance is smaller 
  	// than the original one (Double.MAX_VALUE initially)
  	// this will be in the beginning only true for 
  	// the source vector since its distance is always 0
  	if (minDist < vertex.getValue().get()) {
    		vertex.setValue(new IntWritable(minDist));
    		for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
      			// send a message to all neighbours with 
      			// the current distance from the source vector
      			// + the distance to the neighbour
      			//int distance = minDist + edge.getValue().get();
      			int distance = minDist + 1;
      			sendMessage(edge.getTargetVertexId(), new IntWritable(distance));
    		}
  	}
  
  	// task is finished, until new messages will arrive
  	// in this case the vertex will be woken up again
  	vertex.voteToHalt();

  }
}
