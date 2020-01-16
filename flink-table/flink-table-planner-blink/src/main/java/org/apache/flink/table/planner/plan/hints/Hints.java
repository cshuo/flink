package org.apache.flink.table.planner.plan.hints;

import org.apache.calcite.rel.hint.HintOptionChecker;
import org.apache.calcite.rel.hint.HintStrategy;

/**
 * todo add doc.
 */
public class Hints {

	/**
	 * todo doc.
	 */
	public abstract static class Hint {
		private String hintName;
		private HintCategory hintCategory;
		private HintStrategy hintStrategy;
		private HintOptionChecker hintOptionChecker;

		public Hint(
			String hintName,
			HintCategory hintCategory,
			HintStrategy hintStrategy,
			HintOptionChecker hintOptionChecker) {
			this.hintName = hintName.toLowerCase();
			this.hintCategory = hintCategory;
			this.hintOptionChecker = hintOptionChecker;
			this.hintStrategy = hintStrategy;
		}

		public String getHintName() {
			return hintName;
		}

		public HintStrategy getHintStrategy() {
			return hintStrategy;
		}

		public HintCategory getHintCategory() {
			return hintCategory;
		}

		public HintOptionChecker getHintOptionChecker() {
			return hintOptionChecker;
		}
	}

	/**
	 *  Join hints.
	 **/
	public static class JoinHint extends Hint implements Comparable<JoinHint> {
		private JoinHintType joinHintType;

		public JoinHint(
			String hintName,
			JoinHintType joinType,
			HintStrategy hintStrategy,
			HintOptionChecker hintOptionChecker) {
			super(hintName, HintCategory.JOIN, hintStrategy, hintOptionChecker);
			this.joinHintType = joinType;
		}

		public JoinHintType getJoinHintType() {
			return joinHintType;
		}

		public int compareTo(JoinHint o) {
			return this.joinHintType.compareTo(o.joinHintType);
		}
	}

	/**
	 * todo doc.
	 */
	public enum HintCategory {
		/** Hints for Join. */
		JOIN,
		/** Hints for Resource Constraint. */
		RESOURCE_CONSTRAINT,
		/** Hints for planner feature. */
		PLANNER_FEATURE,
		/** Hints for table scan. */
		TABLE_SCAN
	}

	/**
	 * Types of join (Batch mode).
	 */
	public enum JoinHintType {
		/** Broadcast hash join. */
		BHJ,
		/** Shuffle hash join. */
		SHJ,
		/** Sort merge join. */
		SMJ,
		/** Nest loop join. */
		NLJ,
		/** No hash join. */
		NHJ,
		/** No sort merge join. */
		NMJ,
		/** No nested loop join. */
		NNLJ
	}
}
