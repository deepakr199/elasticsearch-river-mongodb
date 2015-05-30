package org.elasticsearch.river.mongodb.util;

import java.util.Comparator;
import com.mongodb.DBObject;

public class PositionSorter implements Comparator<DBObject> {
	
	public int compare(DBObject obj1, DBObject obj2) {
		//Sort by position
		return (Integer) obj1.get("position") - (Integer) obj2.get("position");

	}
}
