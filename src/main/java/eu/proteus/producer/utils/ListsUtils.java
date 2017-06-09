package eu.proteus.producer.utils;

import java.util.ArrayList;
import java.util.List;

public class ListsUtils {

	/**
	 * Returns a copy of the given list
	 * @param list A list containing some values
	 * @return A copy of the given list
	 */
    public static <T> List<T> copy (List<T> list){
        return new ArrayList<T>(list);
    }
}
