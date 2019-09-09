package com.flex.aws.exceptions;

public class PropertyException extends Exception {

	/**
	 * Serial Version.
	 */
	private static final long serialVersionUID = 8457313895307710705L;

	/**
	 * Constructs a new {@link JacksonConverterException} with the provided
	 * message.
	 *
	 * @param message
	 *            Error message detailing exception
	 */
	public PropertyException(final String message) {
		super("Property not found : " + message);
	}
}
