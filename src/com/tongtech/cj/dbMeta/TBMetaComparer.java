package com.tongtech.cj.dbMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.tongtech.cj.dbMeta.TBMeta.ColMeta;

public class TBMetaComparer {

	/**
	 * 比较两个表的列
	 * 
	 * @param left
	 * @param right
	 * @param compareType
	 *            是否比较列的类型
	 * @param comparePrecision
	 *            是否比较列的精度
	 * @return
	 */
	public static CompareCOLSRS compareCols(TBMeta left, TBMeta right,
			boolean compareType, boolean comparePrecision) {
		CompareCOLSRS compareCOLSRS = new CompareCOLSRS();
		List<ColMeta> leftCols = sortColsByName(left);
		List<ColMeta> rightCols = sortColsByName(right);
		int left_size = leftCols.size();
		int right_size = rightCols.size();
		int left_i = 0;
		int right_i = 0;
		int compare_flag = 0;
		ColMeta left_ColMeta = null;
		ColMeta right_ColMeta = null;
		while (true) {
			if (left_i >= left_size) {
				for (; right_i < right_size; right_i++) {
					compareCOLSRS.onlyRight.add(rightCols.get(right_i));
				}
				break;
			}
			if (right_i >= right_size) {
				for (; left_i < left_size; left_i++) {
					compareCOLSRS.onlyleft.add(leftCols.get(left_i));
				}
				break;
			}
			left_ColMeta = leftCols.get(left_i);
			right_ColMeta = rightCols.get(right_i);
			compare_flag = left_ColMeta.getName().toUpperCase()
					.compareTo(right_ColMeta.getName().toUpperCase());
			if (compare_flag == 0) {
				left_i++;
				right_i++;
				if (compareType
						&& !left_ColMeta.getType().equals(
								right_ColMeta.getType())) {
					compareCOLSRS.diff.add(new ColMeta[] { left_ColMeta,
							right_ColMeta });
					continue;
				}
				if (comparePrecision
						&& (left_ColMeta.getSize() != right_ColMeta.getSize() || left_ColMeta
								.getDigits() != right_ColMeta.getDigits())) {
					compareCOLSRS.diff.add(new ColMeta[] { left_ColMeta,
							right_ColMeta });
					continue;
				}
				compareCOLSRS.jointly.add(left_ColMeta);
			} else if (compare_flag < 0) {
				compareCOLSRS.onlyleft.add(left_ColMeta);
				left_i++;
			} else if (compare_flag > 0) {
				compareCOLSRS.onlyRight.add(right_ColMeta);
				right_i++;
			}
		}
		return compareCOLSRS;
	}

	private static List<ColMeta> sortColsByName(TBMeta tbMeta) {
		if (tbMeta == null) {
			return new ArrayList<ColMeta>();

		}
		List<ColMeta> cols = tbMeta.getCols();
		Collections.sort(cols, new Comparator<ColMeta>() {
			@Override
			public int compare(ColMeta o1, ColMeta o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return cols;
	}

	/**
	 * 比较表的列的结果
	 * 
	 * @author caijun
	 * 
	 */
	public static class CompareCOLSRS {
		List<ColMeta> onlyleft = new ArrayList<ColMeta>();// 只有左边有的列
		List<ColMeta> onlyRight = new ArrayList<ColMeta>();// 只有右边有的列
		List<ColMeta> jointly = new ArrayList<ColMeta>(); // 共有的列
		List<ColMeta[]> diff = new ArrayList<ColMeta[]>(); // 左右都有的列但内部存在差异,0是左边的表,1是右边的表

		public List<ColMeta> getOnlyleft() {
			return onlyleft;
		}

		public List<ColMeta> getOnlyRight() {
			return onlyRight;
		}

		public List<ColMeta> getJointly() {
			return jointly;
		}

		public List<ColMeta[]> getDiff() {
			return diff;
		}
	}
}
