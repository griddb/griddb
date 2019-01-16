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

#ifndef __PV3TEST_H__
#define __PV3TEST_H__

/* -------------------- pv3box  -------------------- */

typedef struct TrPv3Box {
	double p0[3];
	double p1[3];
} TrPv3Box;

void TrPv3Box_disp(TrPv3Box *box);
void TrPv3Vec_disp(double v[]);

/* -------------------- pv3bkey -------------------- */

typedef enum TrPv3Key_type {
	TR_PV3KEY_NONE,
	TR_PV3KEY_CONE,
	TR_PV3KEY_PLANE,
	TR_PV3KEY_SPHERE,
	TR_PV3KEY_CYLINDER,
	/* */
	TR_PV3KEY_NOT,
	TR_PV3KEY_AND,
	TR_PV3KEY_OR
} TrPv3Key_type;

typedef struct TrPv3Key {
	TrPv3Key_type type;
	double A[3][3];
	double b[3];
	double c;
	double p[3];
	int negative;
	struct TrPv3Key *k1, *k2;
} TrPv3Key;

void TrPv3Key_disp(TrPv3Key *key);

TrPv3Key *TrPv3Key_plane(TrPv3Key *key, double p0[], double p1[]);
TrPv3Key *TrPv3Key_sphere(TrPv3Key *key, double p0[], double R);
TrPv3Key *TrPv3Key_cylinder(TrPv3Key *key, double p0[], double p1[], double R);
TrPv3Key *TrPv3Key_cone(TrPv3Key *key, double p0[], double p1[], double A);

TrPv3Key *TrPv3Key_and(TrPv3Key *key, TrPv3Key *k1, TrPv3Key *k2);
TrPv3Key *TrPv3Key_or(TrPv3Key *key, TrPv3Key *k1, TrPv3Key *k2);
TrPv3Key *TrPv3Key_not(TrPv3Key *key);

/* -------------------- TrPv3Out -------------------- */

typedef struct TrPv3Out {
	int d;
	double v;
	double p[3];
} TrPv3Out;

void TrPv3Out_disp(TrPv3Out *out);

/* -------------------- main -------------------- */

int TrPv3Test(TrPv3Box *box, TrPv3Key *key, TrPv3Out *out);

int TrPv3Test2(TrPv3Box *box, TrPv3Key *key);

/* EOF */
#endif
