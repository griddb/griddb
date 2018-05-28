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
package com.toshiba.mwcloud.gs.common;

import java.util.Set;

import com.toshiba.mwcloud.gs.experimental.ContainerAttribute;
import com.toshiba.mwcloud.gs.experimental.ContainerCondition;

public class ContainerKeyPredicate {

	public static final int ATTRIBUTE_BASE = ContainerAttribute.BASE.flag();

	public static final int ATTRIBUTE_SINGLE = ContainerAttribute.SINGLE.flag();

	private static final int[] EMPTY_ATTRIBUTES = new int[0];

	private int[] attributes;

	public ContainerKeyPredicate() {
		this((int[]) null);
	}

	public ContainerKeyPredicate(int[] attributes) {
		setAttributes(attributes);
	}

	public ContainerKeyPredicate(Set<ContainerAttribute> attributes) {
		this(toAttributes(attributes));
	}

	public ContainerKeyPredicate(ContainerCondition cond) {
		this(toAttributes(cond.getAttributes()));
	}

	public static int[] toAttributes(Set<ContainerAttribute> src) {
		if (src == null) {
			return EMPTY_ATTRIBUTES;
		}
		else {
			final int[] dest = new int[src.size()];
			int i = 0;
			for (ContainerAttribute attr : src) {
				dest[i] = attr.flag();
				i++;
			}
			return dest;
		}
	}

	public int[] getAttributes() {
		return attributes;
	}

	public void setAttributes(int[] attributes) {
		if (attributes == null) {
			this.attributes = EMPTY_ATTRIBUTES;
		}
		else {
			this.attributes = attributes;
		}
	}

	public static ContainerKeyPredicate ofDefaultAttributes(boolean unified) {
		final ContainerKeyPredicate pred = new ContainerKeyPredicate();
		pred.setAttributes(new int[] {
				(unified ? ATTRIBUTE_SINGLE : ATTRIBUTE_BASE)
		});
		return pred;
	}

}
