package org.apache.flink.table.planner.plan.hints;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.hint.FlinkHintStrategies;
import org.apache.flink.table.planner.plan.utils.HintUtils;

import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.calcite.hint.FlinkHintStrategies.JOIN_HINT_MATCHER;

/**
 * todo add doc.
 */
public class BuiltInHintTable {

	/**
	 * Broadcast hash join.
	 */
	private static final Hints.Hint USE_BROADCAST =
		new Hints.JoinHint(
			"USE_BROADCAST",
			Hints.JoinHintType.BHJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER)),
			HintUtils.LIST_OPTION_CHECKER);

	/**
	 * Shuffle hash join.
	 */
	private static final Hints.Hint USE_SHUFFLE_HASH =
		new Hints.JoinHint(
			"USE_HASH",
			Hints.JoinHintType.SHJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER)),
			HintUtils.LIST_OPTION_CHECKER);

	/**
	 * Sort merge join.
	 */
	private static final Hints.Hint USE_SORT_MERGE =
		new Hints.JoinHint(
			"USE_MERGE",
			Hints.JoinHintType.SMJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER)),
			HintUtils.LIST_OPTION_CHECKER);

	/**
	 * Nested loop join.
	 */
	private static final Hints.Hint USE_NESTED_LOOP =
		new Hints.JoinHint(
			"USE_NL",
			Hints.JoinHintType.NLJ,
			FlinkHintStrategies.and(FlinkHintStrategies.JOIN,
				FlinkHintStrategies.explicit(JOIN_HINT_MATCHER)),
			HintUtils.LIST_OPTION_CHECKER);

	private static final Hints.Hint NO_HASH =
		new Hints.JoinHint(
			"NO_USE_HASH",
			Hints.JoinHintType.NHJ,
			FlinkHintStrategies.JOIN,
			HintUtils.LIST_OPTION_CHECKER);

	private static final Hints.Hint NO_SORT_MERGE =
		new Hints.JoinHint(
			"NO_USE_MERGE",
			Hints.JoinHintType.NMJ,
			FlinkHintStrategies.JOIN,
			HintUtils.LIST_OPTION_CHECKER);

	private static final Hints.Hint NO_NESTED_LOOP =
		new Hints.JoinHint(
			"NO_USE_NL",
			Hints.JoinHintType.NNLJ,
			FlinkHintStrategies.JOIN,
			HintUtils.LIST_OPTION_CHECKER);

	private static final Hints.Hint RESOURCE_CONSTRAINT =
		new Hints.Hint(
			"RESOURCE",
			Hints.HintCategory.RESOURCE_CONSTRAINT,
			FlinkHintStrategies.or(
				FlinkHintStrategies.CALC,
				FlinkHintStrategies.PROJECT),
			(hint, errorHandler) -> {
				List<String> validResource = Arrays.asList("MEM", "CPU", "GPU");
				boolean isValid = !hint.kvOptions.isEmpty()
					&& HintUtils.isValidOptions(hint.kvOptions.keySet(), validResource);
				return errorHandler.check(isValid,
					"Hint {} only support setting resource MEM, CPU, GPU", hint.hintName);
			}) {};

	private static final Hints.Hint TABLE_PROPERTIES =
		new Hints.Hint(
			"PROPERTIES",
			Hints.HintCategory.TABLE_SCAN,
			FlinkHintStrategies.TABLE_SCAN,
			HintUtils.KV_OPTION_CHECKER) {};

	private static final Hints.Hint STREAM_AGG_STRATEGY =
		new Hints.Hint(
			"AGG_STRATEGY",
			Hints.HintCategory.PLANNER_FEATURE,
			FlinkHintStrategies.or(
				FlinkHintStrategies.AGGREGATE,
				FlinkHintStrategies.STREAM_GROUP_AGGREGATE),
			(hint, errorHandler) -> {
				List<String> validStrategy = Arrays.asList("TWO_PHASE", "ONE_PHASE");
				boolean isValid = hint.listOptions.size() == 1
					&& HintUtils.isValidOptions(hint.listOptions, validStrategy);
				return errorHandler.check(
					isValid,
					"Hint {} only allows single option: [ONE_PHASE, TWO_PHASE]",
					hint.hintName); }) {};

	public static final Map<String, Hints.Hint> BUILT_IN_HINTS = new HashMap<>();

	public static Hints.Hint getHintSpec(RelHint hint) {
		return BUILT_IN_HINTS.get(hint.hintName.toLowerCase());
	}

	private static HintStrategyTable hintStrategyTable;

	public static HintStrategyTable createHintStrategyTable() {
		if (hintStrategyTable != null) {
			return hintStrategyTable;
		}
		HintStrategyTable.Builder builder = HintStrategyTable.builder();
		// Use reflection to register hint into HintStrategyTable.
		BuiltInHintTable builtInHintTable = new BuiltInHintTable();
		for (Field field : BuiltInHintTable.class.getDeclaredFields()) {
			if (!Hints.Hint.class.isAssignableFrom(field.getType())) {
				continue;
			}
			try {
				Hints.Hint hint = (Hints.Hint) field.get(builtInHintTable);
				BUILT_IN_HINTS.put(hint.getHintName(), hint);
				builder.addHintStrategy(
					hint.getHintName(),
					hint.getHintStrategy(),
					hint.getHintOptionChecker());
			} catch (IllegalAccessException e) {
				throw new TableException(e.getMessage());
			}
		}
		hintStrategyTable = builder.build();
		return hintStrategyTable;
	}
}
