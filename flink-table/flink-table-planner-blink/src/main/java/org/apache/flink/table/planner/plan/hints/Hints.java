package org.apache.flink.table.planner.plan.hints;

import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.RelHint;

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

		public Hint(String hintName, HintCategory hintCategory, HintStrategy hintStrategy) {
			this.hintName = hintName.toLowerCase();
			this.hintCategory = hintCategory;
			this.hintStrategy = hintStrategy;
		}

		/**
		 * Validate the content of the {@link RelHint}.
		 */
		public abstract boolean validateHint(RelHint hint);

		public String getHintName() {
			return hintName;
		}

		public HintStrategy getHintStrategy() {
			return hintStrategy;
		}

		public HintCategory getHintCategory() {
			return hintCategory;
		}
	}

	/**
	 *  Join hints.
	 **/
	public abstract static class JoinHint extends Hint implements Comparable<JoinHint> {
		private JoinHintType joinHintType;

		public JoinHint(String hintName, JoinHintType joinType, HintStrategy hintStrategy) {
			super(hintName, HintCategory.JOIN, hintStrategy);
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
