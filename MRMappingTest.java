import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MRMappingTest {
	public static void insertTest(){
		Map<String, Object> map = new HashMap<>();
		map.put("name", "tanyifu3");
		map.put("age", 35);
		map.put("amt", 9999.99);
		List<Map<String, Object>> insList = new ArrayList<>();
		insList.add(map);
		String tableName = "foo";
		MRMapping db = new MRMapping(false); //doesn't begin a transaction
		long seriailId = db.insertS(tableName, insList, true); //support batch insert
		System.out.println("serialId:" + seriailId);
	}

	public static void deleteTest() {
		List<Object> list1 = new ArrayList<Object>() {
			{
				add("BAR");
			}
		};
		new MRMapping(false).freeOperS("delete from foo where name=?", list1);  //doesn't begin a transaction
	}
	
	public static void updateTest() {
		List<Object> list1 = new ArrayList<Object>() {
			{
				add("BAR");
			}
		};
		MRMapping db = new MRMapping(true); //begin transaction
		db.freeOperS("update foo set name=?", list1);
		db.freeOperS("update bar set name=?", list1);
		db.commitS();
	}
	
	public static void longConnectionTest(){
		MRMapping db2 = new MRMapping(true); //begin transaction
		List<Object> list1 = new ArrayList<Object>() {
			{
				add("BAR");
			}
		};
		try {
			db2.freeOperL("update foo set name=?", list1);
			db2.commitL(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static void selectTest(){
		List<Map<String, Object>> selList = new MRMapping(false).selectS("select id, name as myName, age, amt from foo where id=1", null);
		Map<String, Object> map2 = selList.get(0);
		System.out.println("id:" + map2.get("id").toString() + ", age:" + map2.get("age").toString() + ", amt:" + map2.get("amt").toString() + ", name:" + map2.get("myname"));
	}
	public static void dynamicSelectTest(){
		MRMapping db = new MRMapping(false);
		Map<String, Object>map=new HashMap<>();
		map.put("name", "myname");
		List<Object>objList=new ArrayList<>();
		String dynamicSql=MRMapping.getWhereStatement(map, objList);//dynamicSql= "where name=?", objList=list("myname") 
		List<Map<String, Object>> selList=db.selectS("select id, name from foo"+dynamicSql, objList);
	}
			
	public static void main(String[] args) {
		insertTest();
		deleteTest();
		selectTest();
		updateTest();
		longConnectionTest();
		dynamicSelectTest();
	}
}
