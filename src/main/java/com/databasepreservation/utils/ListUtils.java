package com.databasepreservation.utils;

import java.util.Iterator;
import java.util.List;

public final class ListUtils {
	private ListUtils(){}

	public static boolean equals(List<?> fst, List<?> snd){
		if(fst != null && snd != null){
			Iterator<?> ifst = fst.iterator();
			Iterator<?> isnd = fst.iterator();
			while( ifst.hasNext() && isnd.hasNext() ){
				if( !ifst.next().equals(isnd.next()) )
					return false;
			}

			if( !ifst.hasNext() && !isnd.hasNext()){
				return true;
			}
		}else if( fst == null && snd == null ){
			return true;
		}
		return false;
	}
}
