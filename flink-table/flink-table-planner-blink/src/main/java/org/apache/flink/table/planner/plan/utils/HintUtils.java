package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.hints.Hints.Hint;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for {@link Hint}
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
}
