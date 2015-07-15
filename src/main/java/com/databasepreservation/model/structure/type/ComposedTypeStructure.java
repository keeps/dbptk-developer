/**
 *
 */
package com.databasepreservation.model.structure.type;

import java.util.List;
import java.util.Vector;

import com.databasepreservation.utils.ListUtils;

/**
 * A type composed by structuring other type. Any complex type can be
 * constructed with this type (except recursive types).
 *
 * @author Luis Faria
 */
public class ComposedTypeStructure extends Type {
	private List<Type> elements;

	/**
	 * Empty structured type constructor
	 */
	public ComposedTypeStructure() {
		this.elements = new Vector<Type>();
	}

	/**
	 * Structured type constructor
	 *
	 * @param elements
	 *            the sequence of types that compose this type (required).
	 */
	public ComposedTypeStructure(List<Type> elements) {
		this.elements = elements;
	}

	/**
	 * @return the sequence of types that compose this type
	 */
	public List<Type> getElements() {
		return elements;
	}

	/**
	 * @param elements
	 *            the sequence of types that compose this type
	 */
	public void setElements(List<Type> elements) {
		this.elements = elements;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((elements == null) ? 0 : elements.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ComposedTypeStructure other = (ComposedTypeStructure) obj;
		if (elements == null) {
			if (other.elements != null) {
				return false;
			}
		} else if (!ListUtils.equals(elements,other.elements)) {
			return false;
		}
		return true;
	}

}
