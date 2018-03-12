package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

import java.util.*;

public class Join implements BlockOperator {

	private BlockOperator leftChild;
	private BlockOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] left = leftChild.execute();
		DBColumn[] right = rightChild.execute();
		Map<Object, List<Integer>> leftChildMap = initializeMap(left[leftFieldNo]);

		final int size = left.length + right.length;
		List<List<Object>> result = new ArrayList<>(size);
		for (int i=0; i < size; i++) {
			result.add(new ArrayList<>());
		}

		DBColumn targetRight = right[rightFieldNo];
		for(int i=0; i < targetRight.attributes.length; i++) {
			List<Integer> matchingIndices = leftChildMap.get(targetRight.attributes[i]);
			if (matchingIndices != null) {
				// Rows are columns and Columns are rows, to avoid having a third loop
				List<List<Object>> join = joinColumns(left, right, i, matchingIndices);
				for (int j=0; j < join.size(); j++) {
					result.get(j).addAll(join.get(j));
				}
			}
		}

		return createDBColumn(result, left, right);
	}

	private List<List<Object>> joinColumns(DBColumn[] left, DBColumn[] right, int rightIndex, List<Integer> leftIndices) {
		final int size = left.length + right.length;
		List<List<Object>> result = new ArrayList<>(size);
		for (int i=0; i < size; i++) {
			result.add(new ArrayList<>());
		}

		for (Integer leftIndex : leftIndices) {
			Object[] leftRow = getRow(left, leftIndex);
			Object[] rightRow = getRow(right, rightIndex);

			for (int j = 0; j < leftRow.length; j++) {
				result.get(j).add(leftRow[j]);
			}

			for (int j = leftRow.length; j < leftRow.length + rightRow.length; j++) {
				result.get(j).add(rightRow[j - leftRow.length]);
			}
		}
		return result;
	}

	private Map<Object, List<Integer>> initializeMap(DBColumn col) {
		Map<Object, List<Integer>> map = new HashMap<>();
		for (int i=0; i < col.attributes.length; i++) {
			List<Integer> value = map.getOrDefault(col.attributes[i], new ArrayList<>());
			value.add(i);
			map.put(col.attributes[i], value);
		}
		return map;
	}

	private Object[] getRow(DBColumn[] col, int index) {
		Object[] res = new Object[col.length];
		for (int i=0; i < col.length; i++) {
			res[i] = col[i].attributes[index];
		}
		return res;
	}

	private DBColumn[] createDBColumn(List<List<Object>> result, DBColumn[] left, DBColumn[] right) {
		DBColumn[] res = new DBColumn[result.size()];
		for (int i=0; i < res.length; i++) {
			if (i < left.length)
				res[i] = new DBColumn(result.get(i).toArray(), left[i].type);
			else
				res[i] = new DBColumn(result.get(i).toArray(), right[i-left.length].type);
		}
		return res;
	}
}
