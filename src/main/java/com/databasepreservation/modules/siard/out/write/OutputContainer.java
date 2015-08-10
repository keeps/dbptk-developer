package com.databasepreservation.modules.siard.out.write;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class OutputContainer {
	public enum OutputContainerType {
		INSIDE_ARCHIVE, OUTSIDE_ARCHIVE
	}

	private final Path path;
	private final OutputContainerType type;

	public OutputContainer(Path path, OutputContainerType type){
		this.path = path;
		this.type = type;
	}

	public Path getPath(){
		return path;
	}

	public OutputContainerType getType() {
		return type;
	}

	@Override
	public String toString(){
		return new StringBuilder("Container(Type: ")
				.append(type.toString())
				.append(", Path: '")
				.append(path.toString())
				.append("')")
				.toString();
	}
}
