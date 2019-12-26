package org.apache.flink.table.planner.plan.hints;

import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.flink.table.planner.calcite.hint.FlinkHintStrategies;
import org.apache.flink.table.planner.plan.utils.HintUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * todo add doc.
 */
public class BuiltInHintCatalog {

	private static final Hints.Hint BROADCAST_JOIN =
		new Hints.JoinHint(
			"BROADCAST",
			Hints.JoinHintType.BHJ,
			FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return hint.kvOptions.isEmpty() && !hint.listOptions.isEmpty();
			}
		};

	private static final Hints.Hint NO_HASH_JOIN =
		new Hints.JoinHint(
			"NO_HASH_JOIN",
			Hints.JoinHintType.NHJ,
			FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint USE_HASH_JOIN =
		new Hints.JoinHint(
			"USE_HASH_JOIN",
			Hints.JoinHintType.UHJ,
			FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
			}
		};

	private static final Hints.Hint USE_NL =
		new Hints.JoinHint(
			"USE_NL",
			Hints.JoinHintType.UNL,
			FlinkHintStrategies.JOIN) {
			@Override
			public boolean validateHint(RelHint hint) {
				return !hint.listOptions.isEmpty() && hint.kvOptions.isEmpty();
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
		put(BROADCAST_JOIN.getHintName(), BROADCAST_JOIN);
		put(NO_HASH_JOIN.getHintName(), NO_HASH_JOIN);
		put(USE_HASH_JOIN.getHintName(), USE_HASH_JOIN);
		put(USE_NL.getHintName(), USE_NL);
		put(RESOURCE_CONSTRAINT.getHintName(), RESOURCE_CONSTRAINT);
		put(STREAM_AGG_STRATEGY.getHintName(), STREAM_AGG_STRATEGY);
	}};

	public static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategyTable();

	private static HintStrategyTable createHintStrategyTable() {
		HintStrategyTable.Builder builder = HintStrategyTable.builder();
		for (Hints.Hint hint: BUILT_IN_HINTS.values()) {
			builder.addHintStrategy(hint.getHintName(), hint.getHintStrategy());
		}
		return builder.build();
	}
}
