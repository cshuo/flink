package org.apache.flink.table.planner.plan.hints;

import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.flink.table.planner.calcite.hint.FlinkHintStrategies;
import org.apache.flink.table.planner.plan.utils.HintUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.calcite.hint.FlinkHintStrategies.JOIN_HINT_MATCHER;

/**
 * todo add doc.
 */
public class BuiltInHintCatalog {

	/**
	 * Broadcast hash join.
	 */
	private static final Hints.Hint USE_BROADCAST =
		new Hints.JoinHint(
			"USE_BROADCAST",
			Hints.JoinHintType.BHJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER))) {
			@Override
			public boolean validateHint(RelHint hint) {
				return hint.kvOptions.isEmpty() && !hint.listOptions.isEmpty();
			}
		};

	/**
	 * Shuffle hash join.
	 */
	private static final Hints.Hint USE_SHUFFLE_HASH =
		new Hints.JoinHint(
			"USE_HASH",
			Hints.JoinHintType.SHJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER))) {
			@Override
			public boolean validateHint(RelHint hint) {
				return hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
			}
		};

	/**
	 * Sort merge join.
	 */
	private static final Hints.Hint USE_SORT_MERGE =
		new Hints.JoinHint(
			"USE_MERGE",
			Hints.JoinHintType.SMJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER))) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
			}
		};

	/**
	 * Nested loop join.
	 */
	private static final Hints.Hint USE_NESTED_LOOP =
		new Hints.JoinHint(
			"USE_NL",
			Hints.JoinHintType.NLJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER))) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint NO_HASH =
		new Hints.JoinHint("NO_USE_HASH", Hints.JoinHintType.NHJ, FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				// listOptions can be empty or some specified tables to be joined.
				return hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint NO_SORT_MERGE =
		new Hints.JoinHint("NO_USE_MERGE", Hints.JoinHintType.NMJ, FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				// listOptions can be empty or some specified tables to be joined.
				return hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint NO_NESTED_LOOP =
		new Hints.JoinHint("NO_USE_NL", Hints.JoinHintType.NNLJ, FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				// listOptions can be empty or some specified tables to be joined.
				return hint.kvOptions.isEmpty();
			}
		};



	private static final Hints.Hint RESOURCE_CONSTRAINT =
		new Hints.Hint(
			"RESOURCE",
			Hints.HintCategory.RESOURCE_CONSTRAINT,
			FlinkHintStrategies.or(
				FlinkHintStrategies.CALC,
				FlinkHintStrategies.PROJECT)) {
			@Override
			public boolean validateHint(RelHint hint) {
				List<String> validResource = Arrays.asList("MEM", "CPU", "GPU");
				return !hint.kvOptions.isEmpty()
					&& HintUtils.isValidOptions(hint.kvOptions.keySet(), validResource);
			}
		};

	private static final Hints.Hint TABLE_PROPERTIES =
		new Hints.Hint(
			"PROPERTIES",
			Hints.HintCategory.TABLE_SCAN,
			FlinkHintStrategies.TABLE_SCAN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint TABLE_SKEW_INFO =
		new Hints.Hint(
			"SKEW_INFO",
			Hints.HintCategory.TABLE_SCAN,
			FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint STREAM_AGG_STRATEGY =
		new Hints.Hint(
			"AGG_STRATEGY",
			Hints.HintCategory.PLANNER_FEATURE,
			FlinkHintStrategies.or(
			    FlinkHintStrategies.AGGREGATE,
			    FlinkHintStrategies.STREAM_GROUP_AGGREGATE)) {
			@Override
			public boolean validateHint(RelHint hint) {
				List<String> validStrategy = Arrays.asList("TWO_PHASE", "ONE_PHASE");
				return !hint.listOptions.isEmpty()
					&& HintUtils.isValidOptions(hint.listOptions, validStrategy);
			}
		};

	public static final Map<String, Hints.Hint> BUILT_IN_HINTS = new HashMap<String, Hints.Hint>() {{
		put(USE_BROADCAST.getHintName(), USE_BROADCAST);
		put(USE_SHUFFLE_HASH.getHintName(), USE_SHUFFLE_HASH);
		put(USE_SORT_MERGE.getHintName(), USE_SORT_MERGE);
		put(USE_NESTED_LOOP.getHintName(), USE_NESTED_LOOP);
		put(RESOURCE_CONSTRAINT.getHintName(), RESOURCE_CONSTRAINT);
		put(STREAM_AGG_STRATEGY.getHintName(), STREAM_AGG_STRATEGY);
		put(TABLE_PROPERTIES.getHintName(), TABLE_PROPERTIES);
		put(TABLE_SKEW_INFO.getHintName(), TABLE_SKEW_INFO);
		put(NO_HASH.getHintName(), NO_HASH);
		put(NO_SORT_MERGE.getHintName(), NO_SORT_MERGE);
		put(NO_NESTED_LOOP.getHintName(), NO_NESTED_LOOP);
	}};

	public static Hints.Hint getHintSpec(RelHint hint) {
		return BUILT_IN_HINTS.get(hint.hintName.toLowerCase());
	}

	public static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategyTable();

	private static HintStrategyTable createHintStrategyTable() {
		HintStrategyTable.Builder builder = HintStrategyTable.builder();
		for (Hints.Hint hint: BUILT_IN_HINTS.values()) {
			builder.addHintStrategy(hint.getHintName(), hint.getHintStrategy());
		}
		return builder.build();
	}
}
