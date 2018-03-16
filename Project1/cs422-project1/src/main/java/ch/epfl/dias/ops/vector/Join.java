package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	private Map<Object, List<List<Object>>> leftChildMap;
	private DataType[] leftChildData;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();

		leftChildMap = initializeMap(leftChild, leftFieldNo);
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] right = rightChild.next();

		if (right[0].eof) {
			DBColumn[] res = new DBColumn[leftChildData.length + right.length];
			for (int i=0; i < res.length; i++) {
				res[i] = new DBColumn();
			}
			return res;
		}

		List<List<Object>> result = createEmptyList(leftChildData.length + right.length);

		DBColumn targetRight = right[rightFieldNo];
		int i=0;
		while (i < targetRight.attributes.length) {
			List<List<Object>> matchingRows = leftChildMap.get(targetRight.attributes[i]);
			if (matchingRows != null) {
				// Rows are columns and Columns are rows, to avoid having a third loop
				List<List<Object>> join = joinColumns(right, i, matchingRows);
				for (int j=0; j < join.size(); j++) {
					result.get(j).addAll(join.get(j));
				}
			}
			i++;
		}

		return createDBColumn(result, right);
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	private List<List<Object>> joinColumns(DBColumn[] right, int rightIndex, List<List<Object>> matchingRows) {
		List<List<Object>> result = createEmptyList(leftChildData.length + right.length);
		int numberOfRows = matchingRows.get(0).size();
		for (int i=0; i < numberOfRows; i++) {
			Object[] leftRow = new Object[leftChildData.length];
			for (int j=0; j < matchingRows.size(); j++) {
				leftRow[j] = matchingRows.get(j).get(i);
			}
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

	private Map<Object, List<List<Object>>> initializeMap(VectorOperator child, int fieldNo) {
		Map<Object, List<List<Object>>> map = new HashMap<>();
		DBColumn[] columns = child.next();
		leftChildData = new DataType[columns.length];
		for (int i=0; i < columns.length; i++) {
			leftChildData[i] = columns[i].type;
		}

		while(!columns[0].eof) {
			DBColumn col = columns[fieldNo];
			for (int i = 0; i < col.attributes.length; i++) {
				List<List<Object>> value = map.getOrDefault(col.attributes[i], createEmptyList(columns.length));
				Object[] row = getRow(columns, i);
				for (int j=0; j < value.size(); j++) {
					value.get(j).add(row[j]);
				}
				map.put(col.attributes[i], value);
			}
			columns = child.next();
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

	private DBColumn[] createDBColumn(List<List<Object>> result, DBColumn[] right) {
		DBColumn[] res = new DBColumn[result.size()];
		for (int i=0; i < res.length; i++) {
			if (i < leftChildData.length)
				res[i] = new DBColumn(result.get(i).toArray(), leftChildData[i]);
			else
				res[i] = new DBColumn(result.get(i).toArray(), right[i-leftChildData.length].type);
		}
		return res;
	}

	private List<List<Object>> createEmptyList(int size) {
		List<List<Object>> result = new ArrayList<>(size);
		for (int i=0; i < size; i++) {
			result.add(new ArrayList<>());
		}
		return result;
	}
}
