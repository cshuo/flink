package org.apache.flink.table.planner.calcite.hint;

import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.ExplicitHintMatcher;
import org.apache.calcite.rel.hint.HintStrategies;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.List;

import scala.Option;
import scala.Tuple2;

/**
 * todo add doc.
 */
public class FlinkHintStrategies extends HintStrategies {
	public static final HintStrategy STREAM_GROUP_AGGREGATE =
		new FlinkNodeTypeHintStrategy(FlinkNodeTypeHintStrategy.NodeType.STREAM_GROUP_AGGREGTE);

	public static final HintStrategy BATCH_GROUP_AGGREGATE =
		new FlinkNodeTypeHintStrategy(FlinkNodeTypeHintStrategy.NodeType.BATCH_GROUP_AGGREGTE);

	/**
	 * A join hint matcher for an {@link ExplicitTypeStrategy}.
	 */
	public static final ExplicitHintMatcher<RelNode> JOIN_HINT_MATCHER =
		(hint, rel) -> {
			if (!(rel instanceof LogicalJoin)) {
				return false;
			}
			LogicalJoin join = (LogicalJoin) rel;
			final List<String> hintOptions = hint.listOptions;
			Tuple2<Option<String>, Option<String>> tableNames = JoinUtil.getJoinTableNames(join);
			// table name is case sensitive.
			return hintOptions.isEmpty() ||
				tableNames._1.isDefined() && hintOptions.contains(tableNames._1.get()) ||
				tableNames._2.isDefined() && hintOptions.contains(tableNames._2.get());
		};
}
