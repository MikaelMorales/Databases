package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.*;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	private Map<Object, List<List<Object>>> leftChildMap;
	private DataType[] leftChildData;
	private DBColumn[] cacheMatchingRows;
	private int vectorSize;
	private int currentSize = 0;

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
		DBColumn[] intermediateResult = null;
		currentSize = 0;

		if (cacheMatchingRows != null) {
			currentSize = cacheMatchingRows[0].attributes.length;
			if (currentSize == vectorSize) {
				DBColumn[] res = cacheMatchingRows.clone();
				cacheMatchingRows = null;
				return res;
			}

			if (currentSize < vectorSize && cacheMatchingRows[0].eof) {
				DBColumn[] res = cacheMatchingRows.clone();
				cacheMatchingRows = null;
				return res;
			}

			if (currentSize > vectorSize) {
				DBColumn[] res = new DBColumn[cacheMatchingRows.length];
				DBColumn[] newCache = new DBColumn[cacheMatchingRows.length];
				for (int i=0; i < cacheMatchingRows.length; i++) {
					Object[] content = Arrays.copyOfRange(cacheMatchingRows[i].attributes, 0, vectorSize);
					Object[] cache = Arrays.copyOfRange(cacheMatchingRows[i].attributes, vectorSize, currentSize);
					res[i] = new DBColumn(content, cacheMatchingRows[i].type);
					newCache[i] = new DBColumn(cache,cacheMatchingRows[i].type);
					newCache[i].setEOF(cacheMatchingRows[i].eof);
				}
				cacheMatchingRows = newCache.clone();
				return res;
			}

			if (currentSize < vectorSize) {
				intermediateResult = cacheMatchingRows.clone();
				cacheMatchingRows = null;
			}
		}

		DBColumn[] right = rightChild.next();

		if (right[0].eof && right[0].attributes == null) {
			DBColumn[] res = new DBColumn[leftChildData.length + right.length];
			for (int i=0; i < res.length; i++) {
				res[i] = new DBColumn();
			}
			return res;
		}

		List<List<Object>> result = createEmptyList(leftChildData.length + right.length);

		DBColumn targetRight = right[rightFieldNo];
		computeJoin(targetRight, right, result);
		while(!targetRight.eof && currentSize < vectorSize) {
			right = rightChild.next();
			targetRight = right[rightFieldNo];
			computeJoin(targetRight, right, result);
		}

		if (intermediateResult != null && currentSize > vectorSize) {
			DBColumn[] res = createResultAndPopulateCache(result, right, vectorSize - intermediateResult[0].attributes.length);
			return joinDBColumns(intermediateResult, res, false);
		}
		if (intermediateResult == null && currentSize > vectorSize) {
			return createResultAndPopulateCache(result, right, vectorSize);
		}

		boolean eof = false;
		if (targetRight.eof && cacheMatchingRows == null) {
			eof = true;
		}

		DBColumn[] res = createDBColumn(result, right, eof);
		if (intermediateResult != null) {
			return joinDBColumns(intermediateResult, res, eof);
		} else {
			return res;
		}
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	private DBColumn[] createResultAndPopulateCache(List<List<Object>> result, DBColumn[] right, int size) {
		List<List<Object>> res = new ArrayList<>(result.size());
		List<List<Object>> cache = new ArrayList<>(result.size());

		for (List<Object> l : result) {
			res.add(l.subList(0, size));
			cache.add(l.subList(size, l.size()));
		}

		cacheMatchingRows = createDBColumn(cache, right, right[0].eof);
		return createDBColumn(res, right, false);
	}

	private DBColumn[] joinDBColumns(DBColumn[] columns1, DBColumn[] columns2, boolean eof) {
		DBColumn[] res = new DBColumn[columns1.length];
		for (int i=0; i < columns1.length; i++) {
			Object[] concat = new Object[columns1[i].attributes.length + columns2[i].attributes.length];
			System.arraycopy(columns1[i].attributes, 0, concat, 0, columns1[i].attributes.length);
			System.arraycopy(columns2[i].attributes, 0, concat, columns1[i].attributes.length, columns2[i].attributes.length);
			res[i] = new DBColumn(concat, columns1[i].type);
			res[i].setEOF(eof);
		}
		return res;
	}

	private void computeJoin(DBColumn targetRight, DBColumn[] right, List<List<Object>> result) {
		int i=0;
		while (i < targetRight.attributes.length) {
			List<List<Object>> matchingRows = leftChildMap.get(targetRight.attributes[i]);
			if (matchingRows != null) {
				// Rows are columns and Columns are rows, to avoid having a third loop
				List<List<Object>> join = joinColumns(right, i, matchingRows);
				currentSize += join.get(0).size();
				for (int j=0; j < join.size(); j++) {
					result.get(j).addAll(join.get(j));
				}
			}
			i++;
		}
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
		vectorSize = columns[0].attributes != null ? columns[0].attributes.length : 0;
		fillMap(columns, map, fieldNo);
		while(!columns[0].eof) {
			columns = child.next();
			fillMap(columns, map, fieldNo);
		}
		return map;
	}

	private void fillMap(DBColumn[] columns, Map<Object, List<List<Object>>> map, int fieldNo) {
		DBColumn col = columns[fieldNo];
		if (col.attributes != null) {
			for (int i = 0; i < col.attributes.length; i++) {
				List<List<Object>> value = map.getOrDefault(col.attributes[i], createEmptyList(columns.length));
				Object[] row = getRow(columns, i);
				for (int j = 0; j < value.size(); j++) {
					value.get(j).add(row[j]);
				}
				map.put(col.attributes[i], value);
			}
		}
	}

	private Object[] getRow(DBColumn[] col, int index) {
		Object[] res = new Object[col.length];
		for (int i=0; i < col.length; i++) {
			res[i] = col[i].attributes[index];
		}
		return res;
	}

	private DBColumn[] createDBColumn(List<List<Object>> result, DBColumn[] right, boolean eof) {
		DBColumn[] res = new DBColumn[result.size()];
		for (int i=0; i < res.length; i++) {
			if (i < leftChildData.length)
				res[i] = new DBColumn(result.get(i).toArray(), leftChildData[i]);
			else
				res[i] = new DBColumn(result.get(i).toArray(), right[i-leftChildData.length].type);
			res[i].setEOF(eof);
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
