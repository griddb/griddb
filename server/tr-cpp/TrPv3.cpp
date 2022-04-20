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
	@brief collision detection for a quadratic surface and a box
*/
#include "TrPv3.h"
#include "util/type.h"
#include "gs_error.h"

/* */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

/* */
#define EPSI 1.0E-8
#define DISP(x, f) printf(#x ": " #f "\n", x)
#define DELT(i, j) ((i) == (j) ? 1.0 : 0.0)
#define _SET(u, v, w) (((u) << 0) | ((v) << 2) | ((w) << 4))
#define _GET(uvw, i) (0x3 & ((uvw) >> (i << 1)))
#define _PI_ 3.1415926

/* -------------------- common -------------------- */

static void _fatal(const char *msg, int) {
	GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_TR_PV3_FATAL, msg);
}

static double _dot3(double v1[], double v2[]) {
	double y;
	int i;
	for (i = 0, y = 0.0; i < 3; i++) y += v1[i] * v2[i];
	return y;
}

static double _det2(double a[][2]) {
	return a[0][0] * a[1][1] - a[0][1] * a[1][0];
}

static void _sol2(double a[][2], double b[], double x[]) {
	double det;
	det = _det2(a);
	x[0] = (a[1][1] * b[0] - a[0][1] * b[1]) / det;
	x[1] = (-a[1][0] * b[0] + a[0][0] * b[1]) / det;
}

/* -------------------- TrPv3Box  -------------------- */

void TrPv3Box_disp(TrPv3Box *box) {
	printf("----- TrPv3Box@%p -----\n", box);
	printf("p0: [%f,%f,%f]\n", box->p0[0], box->p0[1], box->p0[2]);
	printf("p1: [%f,%f,%f]\n", box->p1[0], box->p1[1], box->p1[2]);
}

void pv3vec_disp(double v[]) {
	printf("vec: [%f,%f,%f]\n", v[0], v[1], v[2]);
}

static void TrPv3Box_conv(TrPv3Box *box, double uvw[], double xyz[]) {
	int i;
	for (i = 0; i < 3; i++) xyz[i] = box->p0[i] + box->p1[i] * uvw[i];
}

/* -------------------- pv3bkey -------------------- */

void TrPv3Key_disp(TrPv3Key *key) {
	printf("----- TrPv3Key@%p -----\n", key);
	printf("type: %d\n", key->type);
	printf("not: %d\n", key->negative);
	printf("A: [[%f,%f,%f],[%f,%f,%f],[%f,%f,%f]]\n", key->A[0][0],
		key->A[0][1], key->A[0][2], key->A[1][0], key->A[1][1], key->A[1][2],
		key->A[2][0], key->A[2][1], key->A[2][2]);
	printf("b: [%f,%f,%f]\n", key->b[0], key->b[1], key->b[2]);
	printf("c: %f\n", key->c);
}

static double TrPv3Key_eval(TrPv3Key *key, double p[]) {
	int i, j;
	double y;
	y = key->c;
	for (i = 0; i < 3; i++) {
		y += 2 * key->b[i] * p[i];
		for (j = 0; j < 3; j++) {
			y += p[i] * key->A[i][j] * p[j];
		}
	}
	return y;
}

static void TrPv3Key_conv(TrPv3Key *key, TrPv3Box *box, TrPv3Key *out) {
	int i, j;
	out->type = key->type;
	out->negative = key->negative;
	out->c = key->c;
	for (i = 0; i < 3; i++) {
		out->b[i] = box->p1[i] * key->b[i];
		out->c += 2 * box->p0[i] * key->b[i];
		for (j = 0; j < 3; j++) {
			out->A[i][j] = box->p1[i] * key->A[i][j] * box->p1[j];
			out->b[i] += box->p1[i] * key->A[i][j] * box->p0[j];
			out->c += box->p0[i] * key->A[i][j] * box->p0[j];
		}
		if (box->p1[i] < EPSI) {
			out->p[i] = 0;  
		}
		else {
			out->p[i] = (key->p[i] - box->p0[i]) / box->p1[i];
		}
	}
}

/* */
static void TrPv3Key_init(TrPv3Key *key) {
	int i, j;
	/* */
	key->type = TR_PV3KEY_NONE;
	for (i = 0; i < 3; i++)
		for (j = 0; j < 3; j++) key->A[i][j] = 0.0;
	for (i = 0; i < 3; i++) key->b[i] = 0.0;
	key->c = 0.0;
	/* */
	for (i = 0; i < 3; i++) key->p[i] = 0.0;
	/* */
	key->negative = 0;
	key->k1 = NULL;
	key->k2 = NULL;
}

TrPv3Key *TrPv3Key_plane(TrPv3Key *key, double p0[], double p1[]) {
	int i, j;
	TrPv3Key_init(key);
	/* */
	key->type = TR_PV3KEY_PLANE;
	for (i = 0; i < 3; i++)
		for (j = 0; j < 3; j++) key->A[i][j] = 0.0;
	for (i = 0; i < 3; i++) key->b[i] = p1[i] / 2;
	key->c = -_dot3(p0, p1);
	/* */
	for (i = 0; i < 3; i++) key->p[i] = p0[i];
	return key;
}

TrPv3Key *TrPv3Key_sphere(TrPv3Key *key, double p0[], double R) {
	int i, j;
	TrPv3Key_init(key);
	/* */
	key->type = TR_PV3KEY_SPHERE;
	for (i = 0; i < 3; i++)
		for (j = 0; j < 3; j++) key->A[i][j] = DELT(i, j);
	for (i = 0; i < 3; i++) key->b[i] = -p0[i];
	key->c = _dot3(p0, p0) - R * R;
	/* */
	for (i = 0; i < 3; i++) key->p[i] = p0[i];
	key->p[0] += R;
	return key;
}

TrPv3Key *TrPv3Key_cylinder(TrPv3Key *key, double p0[], double p1[], double R) {
	double n1[3], pp, np;
	int i, j;
	/* */
	pp = sqrt(_dot3(p1, p1));
	if (pp < EPSI) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
			"Cannot make cylinder from the arguments");
	}
	for (i = 0; i < 3; i++) n1[i] = p1[i] / pp;
	np = _dot3(p0, n1);
	/* */
	TrPv3Key_init(key);
	/* */
	key->type = TR_PV3KEY_CYLINDER;
	for (i = 0; i < 3; i++)
		for (j = 0; j < 3; j++) key->A[i][j] = DELT(i, j) - n1[i] * n1[j];
	for (i = 0; i < 3; i++) key->b[i] = np * n1[i] - p0[i];
	key->c = _dot3(p0, p0) - np * np - R * R;
	/* */
	return key;
}

TrPv3Key *TrPv3Key_cone(TrPv3Key *key, double p0[], double p1[], double A) {
	double n1[3], pp, np, a1, a2;
	int i, j;
	/* */
	a1 = cos((_PI_ / 180) * A);
	a2 = a1 * a1;
	pp = sqrt(_dot3(p1, p1));
	if (pp < EPSI) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT,
			"Cannot make cone from the arguments");
	}
	for (i = 0; i < 3; i++) n1[i] = p1[i] / pp;
	np = _dot3(p0, n1);
	/* */
	TrPv3Key_init(key);
	key->type = TR_PV3KEY_CONE;
	for (i = 0; i < 3; i++)
		for (j = 0; j < 3; j++) key->A[i][j] = a2 * DELT(i, j) - n1[i] * n1[j];
	for (i = 0; i < 3; i++) key->b[i] = np * n1[i] - a2 * p0[i];
	key->c = a2 * _dot3(p0, p0) - np * np;
	/* */
	for (i = 0; i < 3; i++) key->p[i] = p0[i];
	return key;
}

/* */
TrPv3Key *TrPv3Key_and(TrPv3Key *key, TrPv3Key *k1, TrPv3Key *k2) {
	TrPv3Key_init(key);
	key->type = TR_PV3KEY_AND;
	key->k1 = k1;
	key->k2 = k2;
	return key;
}

TrPv3Key *TrPv3Key_or(TrPv3Key *key, TrPv3Key *k1, TrPv3Key *k2) {
	TrPv3Key_init(key);
	key->type = TR_PV3KEY_OR;
	key->k1 = k1;
	key->k2 = k2;
	return key;
}

TrPv3Key *TrPv3Key_not(TrPv3Key *key) {
	switch (key->type) {
	case TR_PV3KEY_AND:
		key->type = TR_PV3KEY_OR;
		TrPv3Key_not(key->k1);
		TrPv3Key_not(key->k2);
		break;
	case TR_PV3KEY_OR:
		key->type = TR_PV3KEY_AND;
		TrPv3Key_not(key->k1);
		TrPv3Key_not(key->k2);
		break;
	case TR_PV3KEY_CONE:
	case TR_PV3KEY_PLANE:
	case TR_PV3KEY_SPHERE:
	case TR_PV3KEY_CYLINDER:
		key->negative = !key->negative;
		break;
	default:
		_fatal("TrPv3Key_not: error\n", -1);
		break;
	}
	return key;
}

/* -------------------- TrPv3Out -------------------- */

/* */
void TrPv3Out_disp(TrPv3Out *out) {
	printf("----- TrPv3Out@%p -----\n", out);
	printf("d: %d\n", out->d);
	printf("p: [%f,%f,%f]\n", out->p[0], out->p[1], out->p[2]);
	printf("v: %f\n", out->v);
}

/* */
static void TrPv3Out_init(TrPv3Out *out) {
	out->d = -1;
	out->v = 0.0;
	out->p[0] = out->p[1] = out->p[2] = 0.0;
}

/* -------------------- main -------------------- */

static int _c0test(TrPv3Box *box, TrPv3Key *key, int uvw, TrPv3Out *out) {
	double u[3];
	int i;
	for (i = 0; i < 3; i++) u[i] = (double)_GET(uvw, i);
	/* */
	out->v = TrPv3Key_eval(key, u);
	if ((key->negative == 0 && out->v < 0.0) ||
		(key->negative != 0 && out->v > 0.0)) {
		out->d = 0;
		TrPv3Box_conv(box, u, out->p);
		return 1;
	}
	return 0;
}

static int _c1test(
	TrPv3Box *box, TrPv3Key *key, int uvw, int var, TrPv3Out *out) {
	double u[3];
	double A1, b1;
	double *A[3], *b;
	int i;
	for (i = 0; i < 3; i++) {
		u[i] = (double)_GET(uvw, i);
		A[i] = key->A[i];
	}
	b = key->b;
	/* */
	switch (var) {
	case 0:
		A1 = A[0][0];
		b1 = -b[0] - A[0][1] * u[1] - A[0][2] * u[2];
		break;
	case 1:
		A1 = A[1][1];
		b1 = -b[1] - A[1][0] * u[0] - A[1][2] * u[2];
		break;
	case 2:
		A1 = A[2][2];
		b1 = -b[2] - A[2][0] * u[0] - A[2][1] * u[1];
		break;
	default:
		A1 = b1 = 0;
		_fatal("_c1test: error\n", -1);
		break;
	}
	/* */
	if (fabs(A1) < EPSI) return 0;
	u[var] = b1 / A1;
	if (u[var] <= 0.0 || u[var] >= 1.0) return 0;
	/* */
	out->v = TrPv3Key_eval(key, u);
	if ((key->negative == 0 && out->v < 0.0) ||
		(key->negative != 0 && out->v > 0.0)) {
		out->d = 1;
		TrPv3Box_conv(box, u, out->p);
		return 1;
	}
	/* */
	return 0;
}

static int _c2test(
	TrPv3Box *box, TrPv3Key *key, int uvw, int fix, TrPv3Out *out) {
	double u[3];
	double A2[2][2], b2[2], u2[2], det;
	double *A[3], *b;
	int i;
	for (i = 0; i < 3; i++) {
		u[i] = (double)_GET(uvw, i);
		A[i] = key->A[i];
	}
	b = key->b;
	/* */
	switch (fix) {
	case 0:
		A2[0][0] = A[1][1];
		A2[0][1] = A[1][2];
		A2[1][0] = A[2][1];
		A2[1][1] = A[2][2];
		b2[0] = -b[1] - A[1][0] * u[0];
		b2[1] = -b[2] - A[2][0] * u[0];
		break;
	case 1:
		A2[0][0] = A[0][0];
		A2[0][1] = A[0][2];
		A2[1][0] = A[2][0];
		A2[1][1] = A[2][2];
		b2[0] = -b[0] - A[0][1] * u[1];
		b2[1] = -b[2] - A[2][1] * u[1];
		break;
	case 2:
		A2[0][0] = A[0][0];
		A2[0][1] = A[0][1];
		A2[1][0] = A[1][0];
		A2[1][1] = A[1][1];
		b2[0] = -b[0] - A[0][2] * u[2];
		b2[1] = -b[1] - A[1][2] * u[2];
		break;
	default:
		A2[0][0] = A2[0][1] = A2[1][0] = A2[1][1] = 0;
		b2[0] = b2[1] = 0;
		_fatal("_c2test: error\n", -2);
		break;
	}
	/* */
	det = _det2(A2);
	if (fabs(det) < EPSI) return 0;
	_sol2(A2, b2, u2);
	for (i = 0; i < 2; i++)
		if (u2[i] <= 0.0 || u2[i] >= 1.0) return 0;
	/* */
	switch (fix) {
	case 0:
		u[1] = u2[0];
		u[2] = u2[1];
		break;
	case 1:
		u[0] = u2[0];
		u[2] = u2[1];
		break;
	case 2:
		u[0] = u2[0];
		u[1] = u2[1];
		break;
	}
	/* */
	out->v = TrPv3Key_eval(key, u);
	if ((key->negative == 0 && out->v < 0.0) ||
		(key->negative != 0 && out->v > 0.0)) {
		out->d = 2;
		TrPv3Box_conv(box, u, out->p);
		return 1;
	}
	/* */
	return 0;
}

static int _c3test(TrPv3Box *box, TrPv3Key *key, TrPv3Out *out) {
	double *u;
	int i;
	/* */
	u = key->p;
	for (i = 0; i < 3; i++)
		if (u[i] <= 0.0 || u[i] >= 1.0) return 0;

	out->v = 0.0;
	out->d = 3;
	TrPv3Box_conv(box, u, out->p);
	return 1;
}

int TrPv3Test(TrPv3Box *box, TrPv3Key *key, TrPv3Out *out) {
	TrPv3Key key1;

	TrPv3Out_init(out);
	TrPv3Key_conv(key, box, &key1);

	if (_c0test(box, &key1, _SET(0, 0, 0), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(0, 0, 1), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(0, 1, 0), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(0, 1, 1), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(1, 0, 0), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(1, 0, 1), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(1, 1, 0), out) != 0) return 0;
	if (_c0test(box, &key1, _SET(1, 1, 1), out) != 0) return 0;

	if (key->type == TR_PV3KEY_PLANE) return -1;

	if (box->p1[0] < EPSI && box->p1[1] < EPSI && box->p1[1] < EPSI) return -1;

	if (_c1test(box, &key1, _SET(3, 0, 0), 0, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(3, 0, 1), 0, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(3, 1, 0), 0, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(3, 1, 1), 0, out) != 0) return 1;

	if (_c1test(box, &key1, _SET(0, 3, 0), 1, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(0, 3, 1), 1, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(1, 3, 0), 1, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(1, 3, 1), 1, out) != 0) return 1;

	if (_c1test(box, &key1, _SET(0, 0, 3), 2, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(0, 1, 3), 2, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(1, 0, 3), 2, out) != 0) return 1;
	if (_c1test(box, &key1, _SET(1, 1, 3), 2, out) != 0) return 1;

	if ((box->p1[0] < EPSI && box->p1[1] < EPSI) ||
		(box->p1[1] < EPSI && box->p1[2] < EPSI) ||
		(box->p1[2] < EPSI && box->p1[0] < EPSI))
		return -1;

	if (_c2test(box, &key1, _SET(0, 3, 3), 0, out) != 0) return 2;
	if (_c2test(box, &key1, _SET(1, 3, 3), 0, out) != 0) return 2;

	if (_c2test(box, &key1, _SET(3, 0, 3), 1, out) != 0) return 2;
	if (_c2test(box, &key1, _SET(3, 1, 3), 1, out) != 0) return 2;

	if (_c2test(box, &key1, _SET(3, 3, 0), 2, out) != 0) return 2;
	if (_c2test(box, &key1, _SET(3, 3, 1), 2, out) != 0) return 2;

	if (box->p1[0] < EPSI || box->p1[1] < EPSI || box->p1[1] < EPSI) return -1;

	if (_c3test(box, &key1, out) != 0) return 3;

	return -1;
}

int TrPv3Test2(TrPv3Box *box, TrPv3Key *key) {
	TrPv3Out out;
	switch (key->type) {
	case TR_PV3KEY_AND:
		return TrPv3Test2(box, key->k1) != 0 && TrPv3Test2(box, key->k2) != 0;
		break;
	case TR_PV3KEY_OR:
		return TrPv3Test2(box, key->k1) != 0 || TrPv3Test2(box, key->k2) != 0;
		break;
	case TR_PV3KEY_NONE:
	case TR_PV3KEY_CONE:
	case TR_PV3KEY_PLANE:
	case TR_PV3KEY_SPHERE:
	case TR_PV3KEY_CYLINDER:
		return TrPv3Test(box, key, &out) >= 0;
		break;
	default:
		_fatal("TrPv3Test: error\n", -1);
		break;
	}
	return 0;
}

/* EOF */
