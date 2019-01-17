/*
	Copyright (c) 2011 TOSHIBA CORPORATION

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*!
	@file
	@brief TR: R-Tree implementation
*/

#include "gs_error.h"
#include "internal.h"

#define NULL_RECT_P(r) (r->xmin > r->xmax)
typedef union TrRectRepresentation {
	TrRectTag r;
	double dbl[6];
} TrRectRepresentation;
#define Dmin(r, i) (((TrRectRepresentation *)(r))->dbl[i])
#define Dmax(r, i) (((TrRectRepresentation *)(r))->dbl[i + 3])

#define NULL_RECT_PTR_CHECK(r)                                        \
	do {                                                              \
		if (r == NULL) {                                              \
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_LIBRARY_MISUSE,  \
				"Tr internal function is called with NULL pointer."); \
		}                                                             \
	} while (0)

/* initialize a rect as (0,0,0)-(0,0,0) */
void TrRect_init(TrRect r) {
	NULL_RECT_PTR_CHECK(r);
	r->xmin = r->ymin = r->zmin = 0.0;
	r->xmax = r->ymax = r->zmax = 0.0;
}

/* initialize a rect as null */
TrRectTag TrRect_null(void) {
	TrRectTag r;
	r.xmin = 1.0;
	r.xmax = -1.0;
	r.ymin = r.ymax = 0.0;
	r.zmin = r.zmax = 0.0;
	return r;
}

/* calculate the volume of the extent */
double TrRect_extent_volume(TrRect r) {
	int32_t i;
	double rad = 0.0;

	NULL_RECT_PTR_CHECK(r);
	if (NULL_RECT_P(r)) return 0.0;
	/* currently, the circumscribing sphere of a rect is used as the extent */
	for (i = 0; i < 3; i++) {
		double s = (Dmax(r, i) - Dmin(r, i)) / 2.0;
		rad += s * s;
	}
	rad = sqrt(rad);
	return rad * rad * rad * 4.0 * M_PI / 3; /* the volume of the unit sphere */
}

/* return a rect that surrounds the two rects */
TrRectTag TrRect_surround(TrRect r1, TrRect r2) {
	int32_t i;
	TrRectRepresentation rpr;
	NULL_RECT_PTR_CHECK(r1);
	NULL_RECT_PTR_CHECK(r2);
	if (NULL_RECT_P(r1)) return *r2;
	if (NULL_RECT_P(r2)) return *r1;
	for (i = 0; i < 3; i++) {
		Dmin(&rpr, i) = Dmin(r1, i) < Dmin(r2, i) ? Dmin(r1, i) : Dmin(r2, i);
		Dmax(&rpr, i) = Dmax(r1, i) > Dmax(r2, i) ? Dmax(r1, i) : Dmax(r2, i);
	}
	return rpr.r;
}

/* determine whether r1 and r2 overlap or not */
int32_t TrRect_overlap_p(TrRect r1, TrRect r2) {
	int32_t i;
	NULL_RECT_PTR_CHECK(r1);
	NULL_RECT_PTR_CHECK(r2);
	if (NULL_RECT_P(r1)) return 0;
	if (NULL_RECT_P(r2)) return 0;
	for (i = 0; i < 3; i++) {
		if (Dmin(r1, i) > Dmax(r2, i) || Dmin(r2, i) > Dmax(r1, i)) return 0;
	}
	return 1;
}

/* determine whether r1 is contained in r2 */
int32_t TrRect_contained_p(TrRect r1, TrRect r2) {
	int32_t i;
	NULL_RECT_PTR_CHECK(r1);
	NULL_RECT_PTR_CHECK(r2);
	if (NULL_RECT_P(r1)) return 1;
	if (NULL_RECT_P(r2)) return 0;
	for (i = 0; i < 3; i++) {
		if (Dmin(r1, i) < Dmin(r2, i) || Dmax(r1, i) > Dmax(r2, i)) return 0;
	}
	return 1;
}

/* dump a rect */
void TrRect_print(TrRect r, int32_t indent) {
	TrUtil_print_indent(indent);
	printf("rect: (%lf,%lf,%lf)-(%lf,%lf,%lf)\n", r->xmin, r->ymin, r->zmin,
		r->xmax, r->ymax, r->zmax);
}
