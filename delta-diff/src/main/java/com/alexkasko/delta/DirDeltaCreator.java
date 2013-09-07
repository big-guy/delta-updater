package com.alexkasko.delta;

import static com.alexkasko.delta.HashUtils.computeSha1;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.util.RandomAccessMode;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nothome.delta.Delta;
import com.nothome.delta.GDiffWriter;
import com.nothome.delta.SeekableSource;

/**
 * Creates ZIP file (or stream) with GDIFF deltas for all changed files and '.index' text file
 * (with '.index_' prefix) with list of unchanged, added, updated and deleted files with SHA1 hash sums
 *
 * @author alexkasko
 * Date: 11/18/11
 */
public class DirDeltaCreator {
    private static final String EMPTY_STRING = "";

    /**
     * Creates patch ZIP file
     *
     * @param oldDir old version of directory
     * @param newDir new version of directory
     * @param out output stream to write patch into
     * @throws IOException on any io or consistency problem
     */
    public void create(String oldDir, String newDir, OutputStream out) throws IOException {
    	FileObject oldDirVFS = asFileObject(oldDir);
    	FileObject newDirVFS = asFileObject(newDir);
        if(!oldDirVFS.exists()) throw new IOException("Bad oldDir argument");
        if(!newDirVFS.exists()) throw new IOException("Bad newDir argument");

        create(oldDirVFS, newDirVFS, out);
    }

    /**
     * Writes zipped patch into provided output stream
     *
     * @param oldDir old version of directory
     * @param newDir new version of directory
     * @param filter IO filter to select files
     * @param patch output stream to write patch into
     * @throws IOException on any io or consistency problem
     */
    void create(FileObject oldDir, FileObject newDir, OutputStream patch) throws IOException {
        FileSelector filter = new AllFileSelector() { 
        	@Override
        	public boolean includeFile(FileSelectInfo fileInfo) {
        		System.out.println(fileInfo.getFile().getName());
        		try {
					if (fileInfo.getFile().getType()!=FileType.FOLDER) {
						return super.includeFile(fileInfo);
					}
				} catch (FileSystemException e) {
					throw new RuntimeException(e);
				}
        		return false;
        	}
        };
        DeltaIndex paths = readDeltaPaths(oldDir, newDir, filter);
        ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(patch));
        writeIndex(paths, out);
        writeCreated(paths.created, newDir, out);
        writeUpdated(paths.updated, oldDir, newDir, out);
        out.close();
    }

    private Collection<FileObject> listFiles(FileObject dir, FileSelector filter) throws IOException {
    	return Arrays.asList(dir.findFiles(filter));
    }

	private FileObject asFileObject(String dir) throws FileSystemException {
		return VFS.getManager().resolveFile(dir);
	}
        
    private DeltaIndex readDeltaPaths(FileObject oldDir, FileObject newDir, FileSelector filter) throws IOException {
        // read files
        Collection<FileObject> oldFiles = listFiles(oldDir, filter);
        Collection<FileObject> newFiles = listFiles(newDir, filter);
        // want to do comparing on strings, without touching FS
        Set<String> oldSet = ImmutableSet.copyOf(Collections2.transform(oldFiles, new Relativiser(oldDir)));
        Set<String> newSet = ImmutableSet.copyOf(Collections2.transform(newFiles, new Relativiser(newDir)));
        // partitioning
        List<String> createdPaths = Ordering.natural().immutableSortedCopy(Sets.difference(newSet, oldSet));
        List<String> existedPaths = Ordering.natural().immutableSortedCopy(Sets.intersection(oldSet, newSet));
        List<String> deletedPaths = Ordering.natural().immutableSortedCopy(Sets.difference(oldSet, newSet));
        // converting
        ImmutableList<IndexEntry.Created> created = ImmutableList.copyOf(Lists.transform(createdPaths, new CreatedIndexer(newDir)));
        ImmutableList<IndexEntry.Deleted> deleted = ImmutableList.copyOf(Lists.transform(deletedPaths, new DeletedIndexer(oldDir)));
        List<IndexEntry> existed = Lists.transform(existedPaths, new ExistedIndexer(oldDir, newDir));
        // partitioning
        ImmutableList<IndexEntry.Updated> updated = ImmutableList.copyOf(Iterables.filter(existed, IndexEntry.Updated.class));
        ImmutableList<IndexEntry.Unchanged> unchanged = ImmutableList.copyOf(Iterables.filter(existed, IndexEntry.Unchanged.class));
        return new DeltaIndex(created, deleted, updated, unchanged);
    }

    private void writeIndex(DeltaIndex paths, ZipOutputStream out) throws IOException {
        // lazy transforms
        out.putNextEntry(new ZipEntry(".index_" + UUID.randomUUID().toString()));
        Gson gson = new GsonBuilder().create();
        Writer writer = new OutputStreamWriter(out, Charset.forName("UTF-8"));
        for (IndexEntry ie : paths.getAll()) {
            gson.toJson(ie, IndexEntry.class, writer);
            writer.write("\n");
        }
        writer.flush();
        out.closeEntry();
    }

    private void writeCreated(List<IndexEntry.Created> paths, FileObject newDir, ZipOutputStream out) throws IOException {
        for(IndexEntry.Created en : paths) {
            out.putNextEntry(new ZipEntry(en.path));
            IOUtils.copy(newDir.resolveFile(en.path).getContent().getInputStream(), out);
            out.closeEntry();
        }
    }

    private void writeUpdated(List<IndexEntry.Updated> paths, FileObject oldDir, FileObject newDir, ZipOutputStream out) throws IOException {
        for(IndexEntry.Updated en : paths) {
            out.putNextEntry(new ZipEntry(en.path + ".gdiff"));
            FileObject source = oldDir.resolveFile(en.path);
            FileObject target = newDir.resolveFile(en.path);
            
            computeDelta(source, target, out);
            out.closeEntry();
        }
    }

    private void computeDelta(FileObject source, FileObject target, OutputStream out) throws IOException {
        OutputStream guarded = new NoCloseOutputStream(out);
        GDiffWriter writer = new GDiffWriter(guarded);
        
        new Delta().compute(new SeekableSourceFileObject(source), target.getContent().getInputStream(), writer);
    }

    private static class SeekableSourceFileObject implements SeekableSource {

    	SeekableSourceFileObject(FileObject source) throws IOException {
            tmpSource = VFS.getManager().resolveFile("tmp://source");
            tmpSource.copyFrom(source, new AllFileSelector());
    		content  = tmpSource.getContent().getRandomAccessContent(RandomAccessMode.READ);
		}
    	
    	private RandomAccessContent content;
    	private FileObject tmpSource;
    	
		@Override
		public void close() throws IOException {
			content.close();
			tmpSource.delete();
		}

		@Override
		public void seek(long pos) throws IOException {
			content.seek(pos);
		}

		@Override
		public int read(ByteBuffer bb) throws IOException {
			int i=0;
			while(i<bb.remaining()) {
				try {
					bb.put(content.readByte());
					i++;
				} catch (EOFException e) {
					if (i==0) return -1;
					else return i;
				}
			}
			return i;
		}
    }
    
    private static class Relativiser implements Function<FileObject, String> {
        private final FileObject parent;

        private Relativiser(FileObject parent) {
            this.parent = parent;
        }

        @Override
        public String apply(FileObject input) {
        	try {
				return parent.getName().getRelativeName(input.getName());
			} catch (FileSystemException e) {
				throw new RuntimeException(e);
			}
        }
    }

    private static class CreatedIndexer implements Function<String, IndexEntry.Created> {
        private final FileObject parent;

        private CreatedIndexer(FileObject parent) {
            this.parent = parent;
        }

        @Override
        public IndexEntry.Created apply(String path) {
        	try {
        		String sha1 = computeSha1(parent.resolveFile(path).getContent().getInputStream());
            	return new IndexEntry.Created(path, EMPTY_STRING, sha1);
        	} catch(FileSystemException e) {
        		throw new RuntimeException(e);
        	}
        }
    }

    private static class DeletedIndexer implements Function<String, IndexEntry.Deleted> {
        private final FileObject parent;

        private DeletedIndexer(FileObject parent) {
            this.parent = parent;
        }

        @Override
        public IndexEntry.Deleted apply(String path) {
        	try {
        		String sha1 = computeSha1(parent.resolveFile(path).getContent().getInputStream());
        		return new IndexEntry.Deleted(path, sha1, EMPTY_STRING);
        	} catch(FileSystemException e) {
        		throw new RuntimeException(e);
        	}
        }
    }

    private static class ExistedIndexer implements Function<String, IndexEntry> {
        private final FileObject oldParent;
        private final FileObject newParent;

        private ExistedIndexer(FileObject oldParent, FileObject newParent) {
            this.oldParent = oldParent;
            this.newParent = newParent;
        }

        @Override
        public IndexEntry apply(String path) {
        	try {
        		FileObject oldChild = oldParent.resolveFile(path);
        		String oldSha1 = computeSha1(oldChild.getContent().getInputStream());
        		
        		FileObject newChild = newParent.resolveFile(path);
	        	String newSha1 = computeSha1(newChild.getContent().getInputStream());

	        	if(oldSha1.equals(newSha1)) {
	                return new IndexEntry.Unchanged(path, oldSha1, newSha1);
	            } else {
	                return new IndexEntry.Updated(path, oldSha1, newSha1);
	            }
        	} catch(FileSystemException e) {
        		throw new RuntimeException(e);
        	}
        }
    }
}
