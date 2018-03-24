package ch.epfl.dias;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class Main {
	public static void main(String[] args) {
		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		DataType[] lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		testTupleAtAtime("- TUPLE AT A TIME WITH NSM", lineitemSchema, orderSchema, true);
		testTupleAtAtime("- TUPLE AT A TIME WITH PAX", lineitemSchema, orderSchema, false);

		// Loaded here since it's the same store for column and vector.
		System.out.println("LOADING THE DATA AS COLUMNS...");
		ColumnStore columnStoreBigLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		columnStoreBigLineItem.load();
		ColumnStore columnStoreBigOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		columnStoreBigOrder.load();
		System.out.println("FINISHED LOADING THE DATA AS COLUMNS !\n");
		testColumnAtAtime(columnStoreBigLineItem, columnStoreBigOrder);
		testVectorAtAtime(columnStoreBigLineItem, columnStoreBigOrder);

		// This is the given main code
		/*
		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
		rowstore.load();

		PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		paxstore.load();

		 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstore);
		 DBTuple currentTuple = scan.next();
		 while (!currentTuple.eof) {
		 	System.out.println(currentTuple.getFieldAsInt(1));
		 	currentTuple = scan.next();
		 }


		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		ch.epfl.dias.ops.block.Scan scan2 = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan2, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		DBColumn[] result = agg.execute();
		int output = result[0].getAsInteger()[0];
		System.out.println(output);
		*/
	}

	private static void testTupleAtAtime(String title, DataType[] lineitemSchema, DataType[] orderSchema, boolean isRowStoreStore) {
		final int tuplePerPages = 400;
		Store storeBigLineItem;
		Store storeBigOrder;
		if (isRowStoreStore) {
			storeBigLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
			storeBigOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
		} else {
			storeBigLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", tuplePerPages);
			storeBigOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", tuplePerPages);
		}
		System.out.println(title);
		System.out.println("    LOADING THE DATA AS TUPLES...");
		storeBigLineItem.load();
		storeBigOrder.load();
		System.out.println("    FINISHED LOADING THE DATA AS TUPLES !\n");

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey < 100; */
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(storeBigLineItem);
		ch.epfl.dias.ops.volcano.Select selLineitem = new ch.epfl.dias.ops.volcano.Select(scanLineitem, BinaryOp.LT, 0, 100);
		ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(selLineitem, new int[]{0,1});

		long start = System.currentTimeMillis();
		proj.open();
		DBTuple result = proj.next();
		while (!result.eof) {
			result = proj.next();
		}
		proj.close();
		long end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey > 100;", start, end);

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey=O.orderkey; */
		ch.epfl.dias.ops.volcano.Scan scanOrder2 = new ch.epfl.dias.ops.volcano.Scan(storeBigOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem2 = new ch.epfl.dias.ops.volcano.Scan(storeBigLineItem);
		ch.epfl.dias.ops.volcano.HashJoin join2 = new ch.epfl.dias.ops.volcano.HashJoin(scanOrder2, scanLineitem2, 0, 0);
		ch.epfl.dias.ops.volcano.Project proj2 = new ch.epfl.dias.ops.volcano.Project(join2, new int[]{0,1});

		start = System.currentTimeMillis();
		proj2.open();
		DBTuple result2 = proj2.next();
		while (!result2.eof) {
			result2 = proj.next();
		}
		proj2.close();
		end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);


		/* SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3; */
		ch.epfl.dias.ops.volcano.Scan scanLineitem3 = new ch.epfl.dias.ops.volcano.Scan(storeBigLineItem);
		ch.epfl.dias.ops.volcano.Select selOrder3 = new ch.epfl.dias.ops.volcano.Select(scanLineitem3, BinaryOp.GT, 0, 3);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg3 = new ch.epfl.dias.ops.volcano.ProjectAggregate(selOrder3, Aggregate.MAX, DataType.INT, 0);

		start = System.currentTimeMillis();
		agg3.open();
		agg3.next();
		agg3.close();
		end = System.currentTimeMillis();
		printResults("SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3;", start, end);

		/* SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey; */
		ch.epfl.dias.ops.volcano.Scan scanOrder4 = new ch.epfl.dias.ops.volcano.Scan(storeBigOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem4 = new ch.epfl.dias.ops.volcano.Scan(storeBigLineItem);
		ch.epfl.dias.ops.volcano.HashJoin join4 = new ch.epfl.dias.ops.volcano.HashJoin(scanOrder4, scanLineitem4, 0, 0);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg4 = new ch.epfl.dias.ops.volcano.ProjectAggregate(join4, Aggregate.COUNT, DataType.INT, 0);
		start = System.currentTimeMillis();
		agg4.open();
		agg4.next();
		agg4.close();
		end = System.currentTimeMillis();
		printResults("SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);
	}

	private static void testColumnAtAtime(ColumnStore columnstoreBigLineItem, ColumnStore columnstoreBigOrder) {
		System.out.println("- COLUMN AT A TIME");

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey < 100; */
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(columnstoreBigLineItem);
		ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.LT, 0, 100);
		ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selLineitem, new int[]{0,1});

		long start = System.currentTimeMillis();
		proj.execute();
		long end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey > 100;", start, end);

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey=O.orderkey; */
		ch.epfl.dias.ops.block.Scan scanOrder2 = new ch.epfl.dias.ops.block.Scan(columnstoreBigOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem2 = new ch.epfl.dias.ops.block.Scan(columnstoreBigLineItem);
		ch.epfl.dias.ops.block.Join join2 = new ch.epfl.dias.ops.block.Join(scanOrder2, scanLineitem2, 0, 0);
		ch.epfl.dias.ops.block.Project proj2 = new ch.epfl.dias.ops.block.Project(join2, new int[]{0,1});

		start = System.currentTimeMillis();
		proj2.execute();
		end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);


		/* SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3; */
		ch.epfl.dias.ops.block.Scan scanLineitem3 = new ch.epfl.dias.ops.block.Scan(columnstoreBigLineItem);
		ch.epfl.dias.ops.block.Select selOrder3 = new ch.epfl.dias.ops.block.Select(scanLineitem3, BinaryOp.GT, 0, 3);
		ch.epfl.dias.ops.block.ProjectAggregate agg3 = new ch.epfl.dias.ops.block.ProjectAggregate(selOrder3, Aggregate.MAX, DataType.INT, 0);

		start = System.currentTimeMillis();
		agg3.execute();
		end = System.currentTimeMillis();
		printResults("SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3;", start, end);

		/* SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey; */
		ch.epfl.dias.ops.block.Scan scanOrder4 = new ch.epfl.dias.ops.block.Scan(columnstoreBigOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem4 = new ch.epfl.dias.ops.block.Scan(columnstoreBigLineItem);
		ch.epfl.dias.ops.block.Join join4 = new ch.epfl.dias.ops.block.Join(scanOrder4, scanLineitem4, 0, 0);
		ch.epfl.dias.ops.block.ProjectAggregate agg4 = new ch.epfl.dias.ops.block.ProjectAggregate(join4, Aggregate.COUNT, DataType.INT, 0);
		start = System.currentTimeMillis();
		agg4.execute();
		end = System.currentTimeMillis();
		printResults("SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);
	}

	private static void testVectorAtAtime(ColumnStore columnstoreBigLineItem, ColumnStore columnstoreBigOrder) {
		System.out.println("- VECTOR AT A TIME");
		final int VECTOR_SIZE = 400;

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey < 100; */
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.LT, 0, 100);
		ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(selLineitem, new int[]{0,1});

		long start = System.currentTimeMillis();
		proj.open();
		DBColumn[] result = proj.next();
		while (!result[0].eof) {
			result = proj.next();
		}
		proj.close();
		long end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L WHERE L.orderkey > 100;", start, end);

		/* SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey=O.orderkey; */
		ch.epfl.dias.ops.vector.Scan scanOrder2 = new ch.epfl.dias.ops.vector.Scan(columnstoreBigOrder, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Scan scanLineitem2 = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Join join2 = new ch.epfl.dias.ops.vector.Join(scanOrder2, scanLineitem2, 0, 0);
		ch.epfl.dias.ops.vector.Project proj2 = new ch.epfl.dias.ops.vector.Project(join2, new int[]{0,1});

		start = System.currentTimeMillis();
		proj2.open();
		DBColumn[] result2 = proj2.next();
		while (!result2[0].eof) {
			result2 = proj.next();
		}
		proj2.close();
		end = System.currentTimeMillis();
		printResults("SELECT L.orderkey, L.partkey FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);


		/* SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3; */
		ch.epfl.dias.ops.vector.Scan scanLineitem3 = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Select selOrder3 = new ch.epfl.dias.ops.vector.Select(scanLineitem3, BinaryOp.GT, 0, 3);
		ch.epfl.dias.ops.vector.ProjectAggregate agg3 = new ch.epfl.dias.ops.vector.ProjectAggregate(selOrder3, Aggregate.MAX, DataType.INT, 0);

		start = System.currentTimeMillis();
		agg3.open();
		agg3.next();
		agg3.close();
		end = System.currentTimeMillis();
		printResults("SELECT MAX(L.orderkey) FROM lineitem_big L WHERE L.orderkey > 3;", start, end);

		/* SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey; */
		ch.epfl.dias.ops.vector.Scan scanOrder4 = new ch.epfl.dias.ops.vector.Scan(columnstoreBigOrder, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Scan scanLineitem4 = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, VECTOR_SIZE);
		ch.epfl.dias.ops.vector.Join join4 = new ch.epfl.dias.ops.vector.Join(scanOrder4, scanLineitem4, 0, 0);
		ch.epfl.dias.ops.vector.ProjectAggregate agg4 = new ch.epfl.dias.ops.vector.ProjectAggregate(join4, Aggregate.COUNT, DataType.INT, 0);
		start = System.currentTimeMillis();
		agg4.open();
		agg4.next();
		agg4.close();
		end = System.currentTimeMillis();
		printResults("SELECT COUNT(L.orderkey) FROM lineitem_big L, orders_big O WHERE L.orderkey = O.orderkey;", start, end);
	}

	private static void printResults(String query, long startWithOpen, long endTime) {
		//System.out.println(query);
		System.out.println("Execution time: " + (endTime - startWithOpen) + " ms");
		System.out.println();
	}
}