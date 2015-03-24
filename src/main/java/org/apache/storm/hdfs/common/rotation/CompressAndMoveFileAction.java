/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.common.rotation;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.enums.CompressionTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressAndMoveFileAction implements MultiFSRotationAction {
	private static final long serialVersionUID = -2854584138871880231L;
	private static final Logger logger = LoggerFactory.getLogger(CompressAndMoveFileAction.class);

	private String destination;
	private CompressionTypeEnum compressionCodec = CompressionTypeEnum.BZIP2;

	public CompressAndMoveFileAction toDestination(String destDir) {
		destination = destDir;
		return this;
	}

	public CompressAndMoveFileAction withCompression(CompressionTypeEnum compressionCodec) {
		this.compressionCodec = compressionCodec;
		return this;
	}

	/**
	 * Copy to HDFS
	 */
	@Override
	public void execute(FileSystem sourceFileSystem, FileSystem targetFileSystem, Path filePath) throws IOException {
		
		/**
		 * Mark as finalized
		 */
		String markedFileName = filePath.toUri().toString() + "." + "finalized";
		try {
			Files.move(Paths.get(filePath.toUri().toString()), Paths.get(markedFileName));
		} catch (FileAlreadyExistsException x) {
			logger.error("Error, file named {} already exists: {}", markedFileName, x.getMessage());
		} catch (IOException x) {
			logger.error("Error when error {}: {}", markedFileName, x.getMessage());
		}
		
		/**
		 * Compress the file
		 */
		String compressedFileName = markedFileName + "." + compressionCodec.getExtension();
		final OutputStream out = new FileOutputStream(compressedFileName);
		CompressorOutputStream cos;
		try {
			cos = new CompressorStreamFactory().createCompressorOutputStream(compressionCodec.getName(), out);
			IOUtils.copy(new FileInputStream(markedFileName), cos);
			cos.close();
		} catch (CompressorException e1) {
			logger.error("Error when compressing file {}: {}", markedFileName, e1.getMessage());
		}

		/**
		 * Copy to HDFS
		 */
		boolean successfulExport = false;
		Path sourcePath = new Path(compressedFileName);
		Path destPath = new Path(destination, sourcePath.getName());
		try {
			// Check if the destination directory already exists
			if (!(targetFileSystem.exists(new Path(destination)))) {
				logger.info("No such destination {}. Creating ", destination);
				targetFileSystem.mkdirs(new Path(destination));
			}

			targetFileSystem.copyFromLocalFile(sourcePath, destPath);
			successfulExport = true;
			logger.error("File %s copied to {}", sourcePath, destPath);
		} catch (Exception e) {
			logger.error("Exception caught! : {}", e);
		}
		
		/**
		 * Delete after successful export
		 */
		if(successfulExport){
			Files.deleteIfExists(Paths.get(markedFileName));
			Files.deleteIfExists(Paths.get(compressedFileName));
		}
		
	}

	public static void main(String[] args) {
		Path sourcePath = new Path("/data/stats/2014_12_12/dasdsa.log");
		Path destPath = new Path(sourcePath.getParent(), sourcePath.getName());
		System.out.println(destPath);
	}

}
