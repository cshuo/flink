package org.apache.flink.table.planner.calcite.hint;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecGroupAggregateBase;

public class FlinkNodeTypeHintStrategy implements HintStrategy {

	/**
	 * todo doc.
	 */
	enum NodeType {
		/**
		 * todo doc.
		 */
		STREAM_GROUP_AGGREGTE(StreamExecGroupAggregateBase.class),
		/**
		 * todo doc.
		 */
		BATCH_GROUP_AGGREGTE(BatchExecGroupAggregateBase.class);

		/** todo */
		private Class<?> relClazz;

		NodeType(Class<?> relClazz) {
			this.relClazz = relClazz;
		}
	}

	private NodeType nodeType;

	public FlinkNodeTypeHintStrategy(NodeType nodeType) {
		this.nodeType = nodeType;
	}

	@Override
	public boolean supportsRel(RelHint hint, RelNode rel) {
		return false;
	}
}
