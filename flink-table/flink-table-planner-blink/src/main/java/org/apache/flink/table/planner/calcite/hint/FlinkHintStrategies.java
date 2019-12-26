package org.apache.flink.table.planner.calcite.hint;

import org.apache.calcite.rel.hint.HintStrategies;
import org.apache.calcite.rel.hint.HintStrategy;

public class FlinkHintStrategies extends HintStrategies {
	public static final HintStrategy STREAM_GROUP_AGGREGATE =
		new FlinkNodeTypeHintStrategy(FlinkNodeTypeHintStrategy.NodeType.STREAM_GROUP_AGGREGTE);

	public static final HintStrategy BATCH_GROUP_AGGREGATE =
		new FlinkNodeTypeHintStrategy(FlinkNodeTypeHintStrategy.NodeType.BATCH_GROUP_AGGREGTE);
}
