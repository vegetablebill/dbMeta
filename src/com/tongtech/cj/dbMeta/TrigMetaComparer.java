package com.tongtech.cj.dbMeta;

import com.caijun.utils.str.StringUtil;

public class TrigMetaComparer {

	public static boolean compare(TrigMeta a, TrigMeta b) {
		String ac = null;
		String bc = null;
		if (a != null) {
			ac = a.getContent();
		}
		if (b != null) {
			bc = b.getContent();
		}
		return StringUtil.equals(ac, bc);
	}

}
