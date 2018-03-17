package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class VolcanoTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;

	RowStore rowstoreBigOrder;
	RowStore rowstoreBigLineItem;


	@Before
    public void init()  {
    	
		schema = new DataType[]{ 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT,
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT };
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem.load();

        rowstoreBigLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		rowstoreBigOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
    }
    
	@Test
	public void spTestData(){
	    /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 1);
	}
	
	@Test
	public void spTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}

	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selOrder, selLineitem, 0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selLineitem, selOrder, 0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}

	@Test
	public void testDoubleMax(){
	    /* SELECT MAX(col4) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 4);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertTrue(output == 49);
	}

	@Test
	public void testDoubleMin(){
	    /* SELECT MIN(col4) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 4);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertTrue(output == 8);
	}

	@Test
	public void testDoubleAvg(){
	    /* SELECT AVG(col4) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 4);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertTrue(output == 30.4);
	}

	@Test
	public void testDoubleSum(){
	    /* SELECT SUM(col4) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 4);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertTrue(output == 304);
	}

	@Test
	public void testIntMax(){
	    /* SELECT MAX(col3) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 76910);
	}

	@Test
	public void testIntMin(){
	    /* SELECT MIN(col3) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 6348);
	}

	@Test
	public void testIntAvg(){
	    /* SELECT AVG(col3) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertTrue(output == 38449.6);
	}

	@Test
	public void testIntSum(){
	    /* SELECT SUM(col3) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 384496);
	}

	@Test
	public void testSelectLT(){
	    /* SELECT COUNT(*) FROM data WHERE col0 < 3 */
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 0, 3);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 7);
	}

	@Test
	public void testSelectLE(){
	    /* SELECT COUNT(*) FROM data WHERE col0 <= 2 */
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LE, 0, 2);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 7);
	}

	@Test
	public void testSelectGT(){
	    /* SELECT COUNT(*) FROM data WHERE col0 > 1 */
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GT, 0, 1);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 4);
	}

	@Test
	public void testSelectGE(){
	    /* SELECT COUNT(*) FROM data WHERE col0 >= 1 */
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 0, 1);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 10);
	}

	@Test
	public void joinTest3(){
	    /* SELECT COUNT(*) FROM lineitem JOIN data ON (o_orderkey = orderkey) WHERE orderkey = 1;*/

		ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);

	    /*Filtering on both sides */
		Select selData = new Select(scanData, BinaryOp.EQ,0,1);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,1);

		HashJoin join = new HashJoin(selData, selLineitem,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 6);
	}

	@Test
	public void joinTest4(){
	    /* SELECT COUNT(*) FROM data JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 1;*/

		ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);

	    /*Filtering on both sides */
		Select selData = new Select(scanData, BinaryOp.EQ,0,1);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,1);
		HashJoin join = new HashJoin(selLineitem, selData,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 6);
	}

	@Test
	public void joinTest5(){
	    /* SELECT COUNT(*) FROM lineitem JOIN data ON (o_orderkey = orderkey)*/

		ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);

		HashJoin join = new HashJoin(scanData, scanLineitem,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 10);
	}

	@Test
	public void joinTest6(){
	    /* SELECT COUNT(*) FROM data JOIN lineitem ON (o_orderkey = orderkey)*/

		ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);

		HashJoin join = new HashJoin(scanLineitem, scanData,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 10);
	}

	@Test
	public void joinTestBig1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreBigOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreBigLineItem);

	    /*Filtering on both sides */
		Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);

		HashJoin join = new HashJoin(selOrder, selLineitem, 0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 6);
	}

	@Test
	public void joinTestBig2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreBigOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreBigLineItem);

	    /*Filtering on both sides */
		Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);

		HashJoin join = new HashJoin(selLineitem, selOrder, 0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 6);
	}

	@Test
	public void testDoubleAvgIntRes(){
        /* SELECT AVG(col4) FROM data*/
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, 4);

		agg.open();

		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 30);
	}

	@Test
	public void testTooSelective() {
        /* SELECT COUNT(*) FROM data where col0 = 15; */
		ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);

		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scanData, BinaryOp.EQ, 0, 15);

		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 0);

		agg.open();
		//This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 0);
	}
}
