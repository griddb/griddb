
/* #line 1 "ebb_request_parser.rl" */
/* This file is part of the libebb web server library
 *
 * Copyright (c) 2008 Ryan Dahl (ry@ndahl.us)
 * All rights reserved.
 *
 * This parser is based on code from Zed Shaw's Mongrel.
 * Copyright (c) 2005 Zed A. Shaw
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
 */
#include "ebb_request_parser.h"

#include <stdio.h>
#include <assert.h>

static int unhex[] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     , 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,-1,-1,-1,-1,-1,-1
                     ,-1,10,11,12,13,14,15,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     ,-1,10,11,12,13,14,15,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
                     };
#define TRUE 1
#define FALSE 0
#define MIN(a,b) (a < b ? a : b)

#define REMAINING (pe - p)
#define CURRENT (parser->current_request)
#define CONTENT_LENGTH (parser->current_request->content_length)
#define CALLBACK(FOR)                               \
  if(CURRENT && parser->FOR##_mark && CURRENT->on_##FOR) {     \
    CURRENT->on_##FOR( CURRENT                      \
                , parser->FOR##_mark                \
                , p - parser->FOR##_mark            \
                );                                  \
 }
#define HEADER_CALLBACK(FOR)                        \
  if(CURRENT && parser->FOR##_mark && CURRENT->on_##FOR) {     \
    CURRENT->on_##FOR( CURRENT                      \
                , parser->FOR##_mark                \
                , p - parser->FOR##_mark            \
                , CURRENT->number_of_headers        \
                );                                  \
 }
#define END_REQUEST                        \
    if(CURRENT && CURRENT->on_complete)               \
      CURRENT->on_complete(CURRENT);       \
    CURRENT = NULL;



/* #line 297 "ebb_request_parser.rl" */



/* #line 2 "ebb_request_parser.c" */
static const char _ebb_request_parser_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	3, 1, 5, 1, 6, 1, 7, 1, 
	8, 1, 9, 1, 11, 1, 12, 1, 
	17, 1, 18, 1, 19, 1, 21, 1, 
	22, 1, 23, 1, 24, 1, 26, 1, 
	27, 1, 28, 1, 29, 1, 30, 1, 
	31, 1, 32, 1, 33, 1, 34, 1, 
	35, 1, 36, 1, 37, 1, 38, 1, 
	39, 2, 1, 7, 2, 2, 9, 2, 
	5, 4, 2, 10, 8, 2, 11, 8, 
	2, 12, 1, 2, 13, 7, 2, 14, 
	6, 2, 15, 7, 2, 16, 7, 2, 
	19, 0, 2, 20, 25, 3, 3, 10, 
	8
};

static const short _ebb_request_parser_key_offsets[] = {
	0, 0, 1, 2, 3, 4, 14, 16, 
	17, 18, 19, 20, 21, 23, 26, 28, 
	31, 32, 52, 53, 69, 71, 72, 73, 
	93, 111, 129, 149, 167, 185, 203, 221, 
	239, 257, 273, 279, 282, 285, 288, 291, 
	292, 295, 298, 301, 303, 306, 309, 312, 
	315, 318, 319, 337, 355, 373, 389, 407, 
	425, 443, 461, 479, 497, 513, 517, 520, 
	538, 556, 574, 592, 610, 628, 646, 662, 
	680, 698, 716, 734, 752, 770, 788, 806, 
	822, 825, 827, 829, 831, 833, 835, 837, 
	839, 840, 849, 858, 864, 870, 880, 889, 
	895, 901, 911, 917, 923, 932, 941, 947, 
	953, 954, 955, 956, 957, 958, 959, 960, 
	961, 962, 963, 964, 965, 966, 967, 968, 
	969, 970, 972, 973, 974, 975, 976, 977, 
	978, 979, 980, 981, 982, 983, 984, 985, 
	986, 989, 990, 991, 992, 993, 994, 996, 
	997, 998, 999, 1000, 1001, 1002, 1003, 1004, 
	1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 
	1013, 1014, 1015, 1016, 1017, 1018, 1025, 1034, 
	1035, 1051, 1052, 1068, 1069, 1077, 1078, 1078, 
	1079, 1080, 1099, 1117, 1134, 1153, 1171, 1188, 
	1198
};

static const char _ebb_request_parser_trans_keys[] = {
	79, 80, 89, 32, 42, 43, 47, 58, 
	45, 57, 65, 90, 97, 122, 32, 35, 
	72, 84, 84, 80, 47, 48, 57, 46, 
	48, 57, 48, 57, 13, 48, 57, 10, 
	13, 33, 67, 84, 99, 116, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 10, 33, 58, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 13, 32, 13, 
	10, 13, 33, 67, 84, 99, 116, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 33, 58, 79, 
	111, 124, 126, 35, 39, 42, 43, 45, 
	46, 48, 57, 65, 90, 94, 122, 33, 
	58, 78, 110, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 78, 84, 110, 116, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 33, 58, 69, 
	101, 124, 126, 35, 39, 42, 43, 45, 
	46, 48, 57, 65, 90, 94, 122, 33, 
	58, 67, 99, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 84, 116, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 33, 58, 73, 105, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 33, 58, 79, 
	111, 124, 126, 35, 39, 42, 43, 45, 
	46, 48, 57, 65, 90, 94, 122, 33, 
	58, 78, 110, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 13, 32, 67, 75, 99, 107, 13, 
	76, 108, 13, 79, 111, 13, 83, 115, 
	13, 69, 101, 13, 13, 69, 101, 13, 
	69, 101, 13, 80, 112, 13, 45, 13, 
	65, 97, 13, 76, 108, 13, 73, 105, 
	13, 86, 118, 13, 69, 101, 13, 33, 
	58, 69, 101, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 78, 110, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 33, 58, 84, 116, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 33, 45, 46, 
	58, 124, 126, 35, 39, 42, 43, 48, 
	57, 65, 90, 94, 122, 33, 58, 76, 
	108, 124, 126, 35, 39, 42, 43, 45, 
	46, 48, 57, 65, 90, 94, 122, 33, 
	58, 69, 101, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 78, 110, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 33, 58, 71, 103, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 33, 58, 84, 
	116, 124, 126, 35, 39, 42, 43, 45, 
	46, 48, 57, 65, 90, 94, 122, 33, 
	58, 72, 104, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 33, 58, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 13, 32, 48, 57, 13, 48, 57, 
	33, 58, 82, 114, 124, 126, 35, 39, 
	42, 43, 45, 46, 48, 57, 65, 90, 
	94, 122, 33, 58, 65, 97, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	66, 90, 94, 122, 33, 58, 78, 110, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 33, 58, 
	83, 115, 124, 126, 35, 39, 42, 43, 
	45, 46, 48, 57, 65, 90, 94, 122, 
	33, 58, 70, 102, 124, 126, 35, 39, 
	42, 43, 45, 46, 48, 57, 65, 90, 
	94, 122, 33, 58, 69, 101, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 33, 58, 82, 114, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 33, 45, 
	46, 58, 124, 126, 35, 39, 42, 43, 
	48, 57, 65, 90, 94, 122, 33, 58, 
	69, 101, 124, 126, 35, 39, 42, 43, 
	45, 46, 48, 57, 65, 90, 94, 122, 
	33, 58, 78, 110, 124, 126, 35, 39, 
	42, 43, 45, 46, 48, 57, 65, 90, 
	94, 122, 33, 58, 67, 99, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 33, 58, 79, 111, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 33, 58, 
	68, 100, 124, 126, 35, 39, 42, 43, 
	45, 46, 48, 57, 65, 90, 94, 122, 
	33, 58, 73, 105, 124, 126, 35, 39, 
	42, 43, 45, 46, 48, 57, 65, 90, 
	94, 122, 33, 58, 78, 110, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 33, 58, 71, 103, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 33, 58, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 13, 32, 
	105, 13, 100, 13, 101, 13, 110, 13, 
	116, 13, 105, 13, 116, 13, 121, 13, 
	32, 37, 60, 62, 127, 0, 31, 34, 
	35, 32, 37, 60, 62, 127, 0, 31, 
	34, 35, 48, 57, 65, 70, 97, 102, 
	48, 57, 65, 70, 97, 102, 43, 58, 
	45, 46, 48, 57, 65, 90, 97, 122, 
	32, 34, 35, 37, 60, 62, 127, 0, 
	31, 48, 57, 65, 70, 97, 102, 48, 
	57, 65, 70, 97, 102, 32, 34, 35, 
	37, 60, 62, 63, 127, 0, 31, 48, 
	57, 65, 70, 97, 102, 48, 57, 65, 
	70, 97, 102, 32, 34, 35, 37, 60, 
	62, 127, 0, 31, 32, 34, 35, 37, 
	60, 62, 127, 0, 31, 48, 57, 65, 
	70, 97, 102, 48, 57, 65, 70, 97, 
	102, 69, 76, 69, 84, 69, 32, 69, 
	84, 32, 69, 65, 68, 32, 79, 67, 
	75, 32, 75, 79, 67, 79, 76, 32, 
	86, 69, 32, 80, 84, 73, 79, 78, 
	83, 32, 79, 82, 85, 83, 84, 32, 
	79, 80, 70, 80, 73, 78, 68, 32, 
	65, 84, 67, 72, 32, 84, 32, 82, 
	65, 67, 69, 32, 78, 76, 79, 67, 
	75, 32, 48, 49, 57, 65, 70, 97, 
	102, 13, 48, 59, 49, 57, 65, 70, 
	97, 102, 10, 13, 33, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 10, 33, 58, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 13, 13, 59, 48, 
	57, 65, 70, 97, 102, 10, 13, 10, 
	13, 32, 33, 59, 61, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 13, 33, 59, 61, 124, 
	126, 35, 39, 42, 43, 45, 46, 48, 
	57, 65, 90, 94, 122, 13, 33, 59, 
	124, 126, 35, 39, 42, 43, 45, 46, 
	48, 57, 65, 90, 94, 122, 13, 32, 
	33, 59, 61, 124, 126, 35, 39, 42, 
	43, 45, 46, 48, 57, 65, 90, 94, 
	122, 13, 33, 59, 61, 124, 126, 35, 
	39, 42, 43, 45, 46, 48, 57, 65, 
	90, 94, 122, 13, 33, 59, 124, 126, 
	35, 39, 42, 43, 45, 46, 48, 57, 
	65, 90, 94, 122, 67, 68, 71, 72, 
	76, 77, 79, 80, 84, 85, 0
};

static const char _ebb_request_parser_single_lengths[] = {
	0, 1, 1, 1, 1, 4, 2, 1, 
	1, 1, 1, 1, 0, 1, 0, 1, 
	1, 8, 1, 4, 2, 1, 1, 8, 
	6, 6, 8, 6, 6, 6, 6, 6, 
	6, 4, 6, 3, 3, 3, 3, 1, 
	3, 3, 3, 2, 3, 3, 3, 3, 
	3, 1, 6, 6, 6, 6, 6, 6, 
	6, 6, 6, 6, 4, 2, 1, 6, 
	6, 6, 6, 6, 6, 6, 6, 6, 
	6, 6, 6, 6, 6, 6, 6, 4, 
	3, 2, 2, 2, 2, 2, 2, 2, 
	1, 5, 5, 0, 0, 2, 7, 0, 
	0, 8, 0, 0, 7, 7, 0, 0, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 2, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	3, 1, 1, 1, 1, 1, 2, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 3, 1, 
	4, 1, 4, 1, 2, 1, 0, 1, 
	1, 7, 6, 5, 7, 6, 5, 10, 
	0
};

static const char _ebb_request_parser_range_lengths[] = {
	0, 0, 0, 0, 0, 3, 0, 0, 
	0, 0, 0, 0, 1, 1, 1, 1, 
	0, 6, 0, 6, 0, 0, 0, 6, 
	6, 6, 6, 6, 6, 6, 6, 6, 
	6, 6, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 6, 6, 6, 5, 6, 6, 
	6, 6, 6, 6, 6, 1, 1, 6, 
	6, 6, 6, 6, 6, 6, 5, 6, 
	6, 6, 6, 6, 6, 6, 6, 6, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 2, 2, 3, 3, 4, 1, 3, 
	3, 1, 3, 3, 1, 1, 3, 3, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 3, 3, 0, 
	6, 0, 6, 0, 3, 0, 0, 0, 
	0, 6, 6, 6, 6, 6, 6, 0, 
	0
};

static const short _ebb_request_parser_index_offsets[] = {
	0, 0, 2, 4, 6, 8, 16, 19, 
	21, 23, 25, 27, 29, 31, 34, 36, 
	39, 41, 56, 58, 69, 72, 74, 76, 
	91, 104, 117, 132, 145, 158, 171, 184, 
	197, 210, 221, 228, 232, 236, 240, 244, 
	246, 250, 254, 258, 261, 265, 269, 273, 
	277, 281, 283, 296, 309, 322, 334, 347, 
	360, 373, 386, 399, 412, 423, 427, 430, 
	443, 456, 469, 482, 495, 508, 521, 533, 
	546, 559, 572, 585, 598, 611, 624, 637, 
	648, 652, 655, 658, 661, 664, 667, 670, 
	673, 675, 683, 691, 695, 699, 706, 715, 
	719, 723, 733, 737, 741, 750, 759, 763, 
	767, 769, 771, 773, 775, 777, 779, 781, 
	783, 785, 787, 789, 791, 793, 795, 797, 
	799, 801, 804, 806, 808, 810, 812, 814, 
	816, 818, 820, 822, 824, 826, 828, 830, 
	832, 836, 838, 840, 842, 844, 846, 849, 
	851, 853, 855, 857, 859, 861, 863, 865, 
	867, 869, 871, 873, 875, 877, 879, 881, 
	883, 885, 887, 889, 891, 893, 898, 905, 
	907, 918, 920, 931, 933, 939, 941, 942, 
	944, 946, 960, 973, 985, 999, 1012, 1024, 
	1035
};

static const unsigned char _ebb_request_parser_indicies[] = {
	0, 1, 2, 1, 3, 1, 4, 1, 
	5, 6, 7, 8, 6, 6, 6, 1, 
	9, 10, 1, 11, 1, 12, 1, 13, 
	1, 14, 1, 15, 1, 16, 1, 17, 
	16, 1, 18, 1, 19, 18, 1, 20, 
	1, 21, 22, 23, 24, 23, 24, 22, 
	22, 22, 22, 22, 22, 22, 22, 1, 
	25, 1, 26, 27, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 29, 30, 28, 
	32, 31, 33, 1, 34, 35, 36, 37, 
	36, 37, 35, 35, 35, 35, 35, 35, 
	35, 35, 1, 26, 27, 38, 38, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 39, 39, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 40, 
	41, 40, 41, 26, 26, 26, 26, 26, 
	26, 26, 26, 1, 26, 27, 42, 42, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	1, 26, 27, 43, 43, 26, 26, 26, 
	26, 26, 26, 26, 26, 1, 26, 27, 
	44, 44, 26, 26, 26, 26, 26, 26, 
	26, 26, 1, 26, 27, 45, 45, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 46, 46, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 47, 
	47, 26, 26, 26, 26, 26, 26, 26, 
	26, 1, 26, 48, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 29, 49, 50, 
	51, 50, 51, 28, 32, 52, 52, 31, 
	32, 53, 53, 31, 32, 54, 54, 31, 
	32, 55, 55, 31, 56, 31, 32, 57, 
	57, 31, 32, 58, 58, 31, 32, 59, 
	59, 31, 32, 60, 31, 32, 61, 61, 
	31, 32, 62, 62, 31, 32, 63, 63, 
	31, 32, 64, 64, 31, 32, 65, 65, 
	31, 66, 31, 26, 27, 67, 67, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 68, 68, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 69, 
	69, 26, 26, 26, 26, 26, 26, 26, 
	26, 1, 26, 70, 26, 27, 26, 26, 
	26, 26, 26, 26, 26, 1, 26, 27, 
	71, 71, 26, 26, 26, 26, 26, 26, 
	26, 26, 1, 26, 27, 72, 72, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 73, 73, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 74, 
	74, 26, 26, 26, 26, 26, 26, 26, 
	26, 1, 26, 27, 75, 75, 26, 26, 
	26, 26, 26, 26, 26, 26, 1, 26, 
	27, 76, 76, 26, 26, 26, 26, 26, 
	26, 26, 26, 1, 26, 77, 26, 26, 
	26, 26, 26, 26, 26, 26, 1, 29, 
	78, 79, 28, 32, 80, 31, 26, 27, 
	81, 81, 26, 26, 26, 26, 26, 26, 
	26, 26, 1, 26, 27, 82, 82, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 83, 83, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 84, 
	84, 26, 26, 26, 26, 26, 26, 26, 
	26, 1, 26, 27, 85, 85, 26, 26, 
	26, 26, 26, 26, 26, 26, 1, 26, 
	27, 86, 86, 26, 26, 26, 26, 26, 
	26, 26, 26, 1, 26, 27, 87, 87, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	1, 26, 88, 26, 27, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 27, 89, 
	89, 26, 26, 26, 26, 26, 26, 26, 
	26, 1, 26, 27, 90, 90, 26, 26, 
	26, 26, 26, 26, 26, 26, 1, 26, 
	27, 91, 91, 26, 26, 26, 26, 26, 
	26, 26, 26, 1, 26, 27, 92, 92, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	1, 26, 27, 93, 93, 26, 26, 26, 
	26, 26, 26, 26, 26, 1, 26, 27, 
	94, 94, 26, 26, 26, 26, 26, 26, 
	26, 26, 1, 26, 27, 95, 95, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	26, 27, 96, 96, 26, 26, 26, 26, 
	26, 26, 26, 26, 1, 26, 97, 26, 
	26, 26, 26, 26, 26, 26, 26, 1, 
	29, 98, 99, 28, 32, 100, 31, 32, 
	101, 31, 32, 102, 31, 32, 103, 31, 
	32, 104, 31, 32, 105, 31, 32, 106, 
	31, 107, 31, 109, 110, 1, 1, 1, 
	1, 1, 108, 112, 113, 1, 1, 1, 
	1, 1, 111, 114, 114, 114, 1, 111, 
	111, 111, 1, 115, 116, 115, 115, 115, 
	115, 1, 9, 1, 10, 117, 1, 1, 
	1, 1, 116, 118, 118, 118, 1, 116, 
	116, 116, 1, 120, 1, 121, 122, 1, 
	1, 123, 1, 1, 119, 124, 124, 124, 
	1, 119, 119, 119, 1, 126, 1, 127, 
	128, 1, 1, 1, 1, 125, 130, 1, 
	131, 132, 1, 1, 1, 1, 129, 133, 
	133, 133, 1, 129, 129, 129, 1, 134, 
	1, 135, 1, 136, 1, 137, 1, 138, 
	1, 139, 1, 140, 1, 141, 1, 142, 
	1, 143, 1, 144, 1, 145, 1, 146, 
	1, 147, 1, 148, 1, 149, 1, 150, 
	1, 151, 152, 1, 153, 1, 154, 1, 
	155, 1, 156, 1, 157, 1, 158, 1, 
	159, 1, 160, 1, 161, 1, 162, 1, 
	163, 1, 164, 1, 165, 1, 166, 1, 
	167, 168, 169, 1, 170, 1, 171, 1, 
	172, 1, 173, 1, 174, 1, 175, 176, 
	1, 177, 1, 178, 1, 179, 1, 180, 
	1, 181, 1, 182, 1, 183, 1, 184, 
	1, 185, 1, 186, 1, 187, 1, 188, 
	1, 189, 1, 190, 1, 191, 1, 192, 
	1, 193, 1, 194, 1, 195, 1, 196, 
	1, 197, 1, 198, 1, 199, 200, 200, 
	200, 1, 201, 199, 202, 200, 200, 200, 
	1, 203, 1, 204, 205, 205, 205, 205, 
	205, 205, 205, 205, 205, 1, 206, 1, 
	205, 207, 205, 205, 205, 205, 205, 205, 
	205, 205, 1, 201, 207, 208, 209, 200, 
	200, 200, 1, 210, 1, 211, 212, 1, 
	213, 1, 208, 209, 214, 209, 215, 214, 
	214, 214, 214, 214, 214, 214, 214, 1, 
	208, 214, 209, 215, 214, 214, 214, 214, 
	214, 214, 214, 214, 1, 208, 215, 209, 
	215, 215, 215, 215, 215, 215, 215, 215, 
	1, 201, 202, 216, 202, 217, 216, 216, 
	216, 216, 216, 216, 216, 216, 1, 201, 
	216, 202, 217, 216, 216, 216, 216, 216, 
	216, 216, 216, 1, 201, 217, 202, 217, 
	217, 217, 217, 217, 217, 217, 217, 1, 
	218, 219, 220, 221, 222, 223, 224, 225, 
	226, 227, 1, 1, 0
};

static const unsigned char _ebb_request_parser_trans_targs[] = {
	2, 0, 3, 4, 5, 6, 93, 97, 
	94, 7, 89, 8, 9, 10, 11, 12, 
	13, 14, 15, 16, 17, 18, 19, 24, 
	63, 183, 19, 20, 21, 22, 20, 21, 
	22, 23, 18, 19, 24, 63, 25, 26, 
	27, 50, 28, 29, 30, 31, 32, 33, 
	34, 34, 35, 40, 36, 37, 38, 39, 
	22, 41, 42, 43, 44, 45, 46, 47, 
	48, 49, 22, 51, 52, 53, 54, 55, 
	56, 57, 58, 59, 60, 61, 61, 62, 
	62, 64, 65, 66, 67, 68, 69, 70, 
	71, 72, 73, 74, 75, 76, 77, 78, 
	79, 80, 80, 81, 82, 83, 84, 85, 
	86, 87, 88, 22, 90, 7, 91, 90, 
	7, 91, 92, 93, 94, 95, 96, 97, 
	7, 89, 98, 100, 99, 101, 7, 89, 
	102, 101, 7, 89, 102, 103, 105, 106, 
	107, 108, 109, 5, 111, 112, 5, 114, 
	115, 116, 5, 118, 119, 120, 5, 122, 
	126, 123, 124, 125, 5, 127, 128, 5, 
	130, 131, 132, 133, 134, 135, 5, 137, 
	140, 152, 138, 139, 5, 141, 142, 143, 
	147, 144, 145, 146, 5, 148, 149, 150, 
	151, 5, 153, 5, 155, 156, 157, 158, 
	5, 160, 161, 162, 163, 164, 5, 166, 
	172, 167, 180, 168, 169, 170, 184, 171, 
	173, 177, 174, 175, 176, 165, 178, 179, 
	181, 182, 1, 104, 110, 113, 117, 121, 
	129, 136, 154, 159
};

static const char _ebb_request_parser_trans_actions[] = {
	0, 0, 0, 0, 37, 9, 9, 71, 
	9, 15, 15, 0, 0, 0, 0, 0, 
	23, 0, 25, 0, 0, 0, 1, 1, 
	1, 98, 0, 11, 3, 65, 0, 0, 
	13, 0, 27, 95, 95, 95, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	11, 0, 3, 3, 0, 0, 0, 0, 
	92, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 89, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 11, 0, 80, 
	21, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 86, 0, 3, 0, 0, 0, 0, 
	0, 0, 0, 83, 5, 68, 5, 0, 
	17, 0, 0, 0, 0, 0, 0, 0, 
	77, 77, 0, 19, 0, 7, 101, 101, 
	7, 0, 74, 74, 0, 0, 0, 0, 
	0, 0, 0, 39, 0, 0, 41, 0, 
	0, 0, 43, 0, 0, 0, 45, 0, 
	0, 0, 0, 0, 47, 0, 0, 49, 
	0, 0, 0, 0, 0, 0, 51, 0, 
	0, 0, 0, 0, 53, 0, 0, 0, 
	0, 0, 0, 0, 55, 0, 0, 0, 
	0, 57, 0, 59, 0, 0, 0, 0, 
	61, 0, 0, 0, 0, 0, 63, 29, 
	29, 0, 0, 0, 0, 0, 33, 0, 
	0, 0, 0, 31, 0, 0, 0, 0, 
	0, 0, 35, 35, 35, 35, 35, 35, 
	35, 35, 35, 35
};

static const int ebb_request_parser_start = 183;
static const int ebb_request_parser_first_final = 183;
static const int ebb_request_parser_error = 0;

static const int ebb_request_parser_en_ChunkedBody = 165;
static const int ebb_request_parser_en_ChunkedBody_chunk_chunk_end = 175;
static const int ebb_request_parser_en_main = 183;


/* #line 300 "ebb_request_parser.rl" */

static void
skip_body(const char **p, ebb_request_parser *parser, size_t nskip) {
  if(CURRENT && CURRENT->on_body && nskip > 0) {
    CURRENT->on_body(CURRENT, *p, nskip);
  }
  if(CURRENT) CURRENT->body_read += nskip;
  parser->chunk_size -= nskip;
  *p += nskip;
  if(0 == parser->chunk_size) {
    parser->eating = FALSE;
    if(CURRENT && CURRENT->transfer_encoding == EBB_IDENTITY) {
      END_REQUEST;
    }
  } else {
    parser->eating = TRUE;
  }
}

void ebb_request_parser_init(ebb_request_parser *parser) 
{
  int cs = 0;
  
/* #line 485 "ebb_request_parser.c" */
	{
	cs = ebb_request_parser_start;
	}

/* #line 323 "ebb_request_parser.rl" */
  parser->cs = cs;

  parser->chunk_size = 0;
  parser->eating = 0;
  
  parser->current_request = NULL;

  parser->header_field_mark = parser->header_value_mark   = 
  parser->query_string_mark = parser->path_mark           = 
  parser->uri_mark          = parser->fragment_mark       = NULL;

  parser->new_request = NULL;
}


/** exec **/
size_t ebb_request_parser_execute(ebb_request_parser *parser, const char *buffer, size_t len)
{
  const char *p, *pe;
  int cs = parser->cs;

  assert(parser->new_request && "undefined callback");

  p = buffer;
  pe = buffer+len;

  if(0 < parser->chunk_size && parser->eating) {
    /* eat body */
    size_t eat = MIN(len, parser->chunk_size);
    skip_body(&p, parser, eat);
  } 

  if(parser->header_field_mark)   parser->header_field_mark   = buffer;
  if(parser->header_value_mark)   parser->header_value_mark   = buffer;
  if(parser->fragment_mark)       parser->fragment_mark       = buffer;
  if(parser->query_string_mark)   parser->query_string_mark   = buffer;
  if(parser->path_mark)           parser->path_mark           = buffer;
  if(parser->uri_mark)            parser->uri_mark            = buffer;

  
/* #line 488 "ebb_request_parser.c" */
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
	if ( cs == 0 )
		goto _out;
_resume:
	_keys = _ebb_request_parser_trans_keys + _ebb_request_parser_key_offsets[cs];
	_trans = _ebb_request_parser_index_offsets[cs];

	_klen = _ebb_request_parser_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _ebb_request_parser_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _ebb_request_parser_indicies[_trans];
	cs = _ebb_request_parser_trans_targs[_trans];

	if ( _ebb_request_parser_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _ebb_request_parser_actions + _ebb_request_parser_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 0:
/* #line 73 "ebb_request_parser.rl" */
	{ parser->header_field_mark   = p; }
	break;
	case 1:
/* #line 74 "ebb_request_parser.rl" */
	{ parser->header_value_mark   = p; }
	break;
	case 2:
/* #line 75 "ebb_request_parser.rl" */
	{ parser->fragment_mark       = p; }
	break;
	case 3:
/* #line 76 "ebb_request_parser.rl" */
	{ parser->query_string_mark   = p; }
	break;
	case 4:
/* #line 77 "ebb_request_parser.rl" */
	{ parser->path_mark           = p; }
	break;
	case 5:
/* #line 78 "ebb_request_parser.rl" */
	{ parser->uri_mark            = p; }
	break;
	case 6:
/* #line 80 "ebb_request_parser.rl" */
	{ 
    HEADER_CALLBACK(header_field);
    parser->header_field_mark = NULL;
  }
	break;
	case 7:
/* #line 85 "ebb_request_parser.rl" */
	{
    HEADER_CALLBACK(header_value);
    parser->header_value_mark = NULL;
  }
	break;
	case 8:
/* #line 90 "ebb_request_parser.rl" */
	{ 
    CALLBACK(uri);
    parser->uri_mark = NULL;
  }
	break;
	case 9:
/* #line 95 "ebb_request_parser.rl" */
	{ 
    CALLBACK(fragment);
    parser->fragment_mark = NULL;
  }
	break;
	case 10:
/* #line 100 "ebb_request_parser.rl" */
	{ 
    CALLBACK(query_string);
    parser->query_string_mark = NULL;
  }
	break;
	case 11:
/* #line 105 "ebb_request_parser.rl" */
	{
    CALLBACK(path);
    parser->path_mark = NULL;
  }
	break;
	case 12:
/* #line 110 "ebb_request_parser.rl" */
	{
    if(CURRENT){
      CURRENT->content_length *= 10;
      CURRENT->content_length += *p - '0';
    }
  }
	break;
	case 13:
/* #line 117 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->transfer_encoding = EBB_IDENTITY; }
	break;
	case 14:
/* #line 118 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->transfer_encoding = EBB_CHUNKED; }
	break;
	case 15:
/* #line 120 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->keep_alive = TRUE; }
	break;
	case 16:
/* #line 121 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->keep_alive = FALSE; }
	break;
	case 17:
/* #line 131 "ebb_request_parser.rl" */
	{
    if(CURRENT) {
      CURRENT->version_major *= 10;
      CURRENT->version_major += *p - '0';
    }
  }
	break;
	case 18:
/* #line 138 "ebb_request_parser.rl" */
	{
  	if(CURRENT) {
      CURRENT->version_minor *= 10;
      CURRENT->version_minor += *p - '0';
    }
  }
	break;
	case 19:
/* #line 145 "ebb_request_parser.rl" */
	{
    if(CURRENT) CURRENT->number_of_headers++;
  }
	break;
	case 20:
/* #line 149 "ebb_request_parser.rl" */
	{
    if(CURRENT && CURRENT->on_headers_complete)
      CURRENT->on_headers_complete(CURRENT);
  }
	break;
	case 21:
/* #line 154 "ebb_request_parser.rl" */
	{
    parser->chunk_size *= 16;
    parser->chunk_size += unhex[(int)*p];
  }
	break;
	case 22:
/* #line 159 "ebb_request_parser.rl" */
	{
    skip_body(&p, parser, MIN(parser->chunk_size, REMAINING));
    p--; 
    if(parser->chunk_size > REMAINING) {
      {p++; goto _out; }
    } else {
      {cs = 175; goto _again;} 
    }
  }
	break;
	case 23:
/* #line 169 "ebb_request_parser.rl" */
	{
    END_REQUEST;
    cs = 183;
  }
	break;
	case 24:
/* #line 174 "ebb_request_parser.rl" */
	{
    assert(CURRENT == NULL);
    CURRENT = parser->new_request(parser->data);
  }
	break;
	case 25:
/* #line 179 "ebb_request_parser.rl" */
	{
    if(CURRENT) { 
      if(CURRENT->transfer_encoding == EBB_CHUNKED) {
        cs = 165;
      } else {
        /* this is pretty stupid. i'd prefer to combine this with skip_chunk_data */
        parser->chunk_size = CURRENT->content_length;
        p += 1;  
        skip_body(&p, parser, MIN(REMAINING, CURRENT->content_length));
        p--;
        if(parser->chunk_size > REMAINING) {
          {p++; goto _out; }
        } 
      }
    }
  }
	break;
	case 26:
/* #line 228 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_COPY;      }
	break;
	case 27:
/* #line 229 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_DELETE;    }
	break;
	case 28:
/* #line 230 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_GET;       }
	break;
	case 29:
/* #line 231 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_HEAD;      }
	break;
	case 30:
/* #line 232 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_LOCK;      }
	break;
	case 31:
/* #line 233 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_MKCOL;     }
	break;
	case 32:
/* #line 234 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_MOVE;      }
	break;
	case 33:
/* #line 235 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_OPTIONS;   }
	break;
	case 34:
/* #line 236 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_POST;      }
	break;
	case 35:
/* #line 237 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_PROPFIND;  }
	break;
	case 36:
/* #line 238 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_PROPPATCH; }
	break;
	case 37:
/* #line 239 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_PUT;       }
	break;
	case 38:
/* #line 240 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_TRACE;     }
	break;
	case 39:
/* #line 241 "ebb_request_parser.rl" */
	{ if(CURRENT) CURRENT->method = EBB_UNLOCK;    }
	break;
/* #line 751 "ebb_request_parser.c" */
		}
	}

_again:
	if ( cs == 0 )
		goto _out;
	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	_out: {}
	}

/* #line 363 "ebb_request_parser.rl" */

  parser->cs = cs;

  HEADER_CALLBACK(header_field);
  HEADER_CALLBACK(header_value);
  CALLBACK(fragment);
  CALLBACK(query_string);
  CALLBACK(path);
  CALLBACK(uri);

  assert(p <= pe && "buffer overflow after parsing execute");

  return(p - buffer);
}

int ebb_request_parser_has_error(ebb_request_parser *parser) 
{
  return parser->cs == ebb_request_parser_error;
}

int ebb_request_parser_is_finished(ebb_request_parser *parser) 
{
  return parser->cs == ebb_request_parser_first_final;
}

void ebb_request_init(ebb_request *request)
{
  request->expect_continue = FALSE;
  request->body_read = 0;
  request->content_length = 0;
  request->version_major = 0;
  request->version_minor = 0;
  request->number_of_headers = 0;
  request->transfer_encoding = EBB_IDENTITY;
  request->keep_alive = -1;

  request->on_complete = NULL;
  request->on_headers_complete = NULL;
  request->on_body = NULL;
  request->on_header_field = NULL;
  request->on_header_value = NULL;
  request->on_uri = NULL;
  request->on_fragment = NULL;
  request->on_path = NULL;
  request->on_query_string = NULL;
}

int ebb_request_should_keep_alive(ebb_request *request)
{
  if(request->keep_alive == -1)
    if(request->version_major == 1)
      return (request->version_minor != 0);
    else if(request->version_major == 0)
      return FALSE;
    else
      return TRUE;
  else
    return request->keep_alive;
}
