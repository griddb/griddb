/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs;

/**
 * Represents the constraints regarding relationship between each two spatial
 * ranges.
 *
 * <p>It is used to specify the conditions to spatial range query.</p>
 */
public enum GeometryOperator {

	/**
	 * Represents that each spatial ranges or their bounding volume are
	 * intersected.
	 *
	 * <p>Represents that each MBBs (Minimum Bounding Box) or MBB and quadric
	 * surface are intersected.
	 * The conditions for determination of the intersections are the same as
	 * {@code ST_MBRIntersects} or {@code ST_QSFMBRIntersects} in TQL.</p>
	 */
	INTERSECT

}
