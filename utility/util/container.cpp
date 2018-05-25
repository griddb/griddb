/*
    Copyright (c) 2017 TOSHIBA Digital Solutions Corporation
    
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
#include "util/container.h"

namespace util {


InsertionResetter::~InsertionResetter() {
	reset();
}

void InsertionResetter::release() throw() {
	entry_ = Entry();
}

void InsertionResetter::reset() throw() {
	if (entry_.func_ != NULL) {
		entry_.func_(entry_.container_, entry_.pos_);
		release();
	}
}

InsertionResetter::Entry::Entry() :
		func_(NULL), container_(NULL), pos_(0) {
}

InsertionResetter::Entry::Entry(
		ResetterFunc func, void *container, size_t pos) :
		func_(func), container_(container), pos_(pos) {
}

} 
