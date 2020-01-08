package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.hints.BuiltInHintCatalog;
import org.apache.flink.table.planner.plan.hints.Hints.Hint;
import org.apache.flink.table.planner.plan.hints.Hints.HintCategory;
import org.apache.flink.table.planner.plan.hints.Hints.JoinHint;
import org.apache.flink.table.planner.plan.hints.Hints.JoinHintType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.flink.table.planner.plan.hints.Hints.JoinHintType.BHJ;
import static org.apache.flink.table.planner.plan.hints.Hints.JoinHintType.NLJ;
import static org.apache.flink.table.planner.plan.hints.Hints.JoinHintType.SHJ;
import static org.apache.flink.table.planner.plan.hints.Hints.JoinHintType.SMJ;

/**
 * Utilities for {@link Hint}.
 */
public class HintUtils {

	/**
	 * Check whether the hints in SQL are valid.
	 */
	public static boolean isValidOptions(Collection<String> keys, List<String> options) {
		options = options.stream().map(String::toLowerCase).collect(Collectors.toList());
		for (String key: keys) {
			if (!options.contains(key.toLowerCase())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Validate user hints in SQL according to
	 * the specification of hints in {@link BuiltInHintCatalog}.
	 */
	public static void validateHint(RelHint hint) {
		Hint hintSpec = BuiltInHintCatalog.getHintSpec(hint);
		if (!hintSpec.validateHint(hint)) {
			throw new IllegalArgumentException("Invalid hint options: " + hint.toString());
		}
	}

	/**
	 * A {@link RelNode} may contains multiple hints with same hint name,
	 * in which case the hint with shortest inherit path is reserved.
	 */
	public static Collection<RelHint> eliminateCommonHints(Collection<RelHint> hints) {
		Map<String, RelHint> hintMap = new HashMap<>();
		List<String> hintNames = new ArrayList<>();
		for (RelHint hint: hints) {
			if (hintNames.contains(hint.hintName)) {
				RelHint curHint = hintMap.get(hint.hintName);
				if (hint.inheritPath.size() < curHint.inheritPath.size()) {
					hintMap.put(hint.hintName, hint);
				}
			} else {
				hintNames.add(hint.hintName);
				hintMap.put(hint.hintName, hint);
			}
		}
		return hintMap.values();
	}

	/**
	 * Only table properties hints are supported currently.
	 */
	public static Map<String, String> getHintedTableProperties(Collection<RelHint> hints) {
		Map<String, String> properties = new HashMap<>();
		for (RelHint hint: eliminateCommonHints(hints)) {
			properties.putAll(hint.kvOptions);
		}
		return properties;
	}

	/**
	 * Returns an join operation {@link OperatorType} specified in join Hints,
	 * which is also must be applicable to the {@link Join} node, taking the
	 * global table config and join info into account.
	 * If multiple join operation hints are supplied, they are prioritized as
	 * "BHJ" over "SMJ" over "SHJ" over "NLJ".
	 */
	public static Optional<JoinHintType> getApplicableJoinHintType(
		Join join, TableConfig tableConfig) {

		Collection<RelHint> hints = eliminateCommonHints(join.getHints());
		Set<JoinHint> joinHints = new TreeSet<>();
		for (RelHint hint: hints) {
			Hint hintSpec = BuiltInHintCatalog.getHintSpec(hint);
			if (hintSpec != null &&
				HintCategory.JOIN == hintSpec.getHintCategory()) {
				joinHints.add((JoinHint) hintSpec);
			}
		}

		for (JoinHint hintSpec: joinHints) {
			switch (hintSpec.getJoinHintType()) {
				case BHJ:
					if (JoinUtil.isJoinTypeApplicable(
						join, OperatorType.BroadcastHashJoin, tableConfig)) {
						return Optional.of(BHJ);
					}
					break;
				case SHJ:
					if (JoinUtil.isJoinTypeApplicable(
						join, OperatorType.ShuffleHashJoin, tableConfig)) {
						return Optional.of(SHJ);
					}
					break;
				case SMJ:
					if (JoinUtil.isJoinTypeApplicable(
						join, OperatorType.SortMergeJoin, tableConfig)) {
						return Optional.of(SMJ);
					}
					break;
				case NLJ:
					if (JoinUtil.isJoinTypeApplicable(
						join, OperatorType.NestedLoopJoin, tableConfig)) {
						return Optional.of(NLJ);
					}
					break;
				default:
					return Optional.of(hintSpec.getJoinHintType());
			}
		}
		return Optional.empty();
	}

	/**
	 * Get the table name specified in the hint for the Join node,
	 * may be one or two table names.
	 *
	 * @param left Table name of the left input of a Join.
	 * @param right Table name of the right input of a Join.
	 * @param hints Hints in a Join.
	 * @param joinHintType Join type of the applicable join Hint.
	 * @return
	 */
	public static List<String> getJoinHintContent(
		Option<String> left,
		Option<String> right,
		List<RelHint> hints,
		JoinHintType joinHintType) {
		// there may exists more than 2 table names in the hint
		// filter out the table names of the join inputs.
		List<String> result = new ArrayList<>();
		for (RelHint hint: eliminateCommonHints(hints)) {
			Hint hintSpec = BuiltInHintCatalog.getHintSpec(hint);
			if (hintSpec.getHintCategory() == HintCategory.JOIN
				&& ((JoinHint) hintSpec).getJoinHintType() == joinHintType) {
				if (left.isDefined() && hint.listOptions.contains(left.get())) {
					result.add(left.get());
				}
				if (right.isDefined() && hint.listOptions.contains(right.get())) {
					result.add(right.get());
				}
			}
		}
		return result;
	}

	/**
	 * Check whether the specified {@link JoinHint} is negative join hint,
	 * e.g., NO_HASH or NO_MERGE or NO_NL.
	 */
	public static boolean isNegativeJoinHint(JoinHintType joinHintType) {
		return joinHintType.compareTo(NLJ) > 0;
	}
}
