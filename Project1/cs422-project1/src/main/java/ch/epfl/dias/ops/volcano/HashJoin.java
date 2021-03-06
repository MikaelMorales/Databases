package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {
	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	private Map<Object, List<DBTuple>> hashMap = new HashMap<>();
	private List<DBTuple> cache = new ArrayList<>();
	private DBTuple currentRow;
	private int cacheOffset;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();

		hashMap = initializeMap(leftChild, leftFieldNo);
	}

	@Override
	public DBTuple next() {
		if (cacheOffset >= cache.size()) { // Done with the cache matching, we can read a new tuple
			currentRow = rightChild.next();
			cacheOffset = 0;

			while(!currentRow.eof && !hashMap.containsKey(currentRow.fields[rightFieldNo])) {
				currentRow = rightChild.next();
			}

			if (currentRow.eof)
				return new DBTuple();

			cache = hashMap.get(currentRow.fields[rightFieldNo]);
		}

		Object[] joinedRow = Stream.concat(Arrays.stream(cache.get(cacheOffset).fields),
				Arrays.stream(currentRow.fields)).toArray(Object[]::new);

		DataType[] joinedType = Stream.concat(Arrays.stream(cache.get(cacheOffset).types),
				Arrays.stream(currentRow.types)).toArray(DataType[]::new);

		cacheOffset++;
		return new DBTuple(joinedRow, joinedType);
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	private Map<Object, List<DBTuple>> initializeMap(VolcanoOperator child, int fieldNo) {
		Map<Object, List<DBTuple>> map = new HashMap<>();
		DBTuple row = child.next();
		while (!row.eof) {
			List<DBTuple> value = map.getOrDefault(row.fields[fieldNo], new ArrayList<>());
			value.add(row);
			map.put(row.fields[fieldNo], value);
			row = child.next();
		}
		return map;
	}
}