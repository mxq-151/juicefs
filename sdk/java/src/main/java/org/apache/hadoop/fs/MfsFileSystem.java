package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class MfsFileSystem extends FilterFileSystem {
    protected String myScheme;
    protected String myAuthority;
    protected URI myUri;
    protected String hdfsScheme;
    protected String hdfsAuthority;
    protected URI hdfsUri;

    protected String jfsScheme;
    protected String jfsAuthority;
    protected URI jfsUri;

    protected FileSystem jfs;


    protected Path swizzleHdfsPath(Path p) {
        String pathUriString = p.toUri().toString();
        URI newPathUri = URI.create(pathUriString);
        return new Path(this.hdfsScheme, this.hdfsAuthority, newPathUri.getPath());
    }

    protected Path swizzleJfsPath(Path p) {
        String pathUriString = p.toUri().toString();
        URI newPathUri = URI.create(pathUriString);
        return new Path(this.jfsScheme, this.jfsAuthority, newPathUri.getPath());
    }

    private Path swizzleReturnPath(Path p) {
        String pathUriString = p.toUri().toString();
        URI newPathUri = URI.create(pathUriString);
        return new Path(this.myScheme, this.myAuthority, newPathUri.getPath());
    }

    protected FileStatus swizzleFileStatus(FileStatus orig, boolean isParam,boolean jfs) {
        Path path=null;
        if(jfs)
        {
            path= this.swizzleJfsPath(orig.getPath());
        }else {
           path= this.swizzleHdfsPath(orig.getPath());
        }

        FileStatus ret = new FileStatus(orig.getLen(), orig.isDir(), orig.getReplication(), orig.getBlockSize(), orig.getModificationTime(), orig.getAccessTime(), orig.getPermission(), orig.getOwner(), orig.getGroup(), isParam ? path : this.swizzleReturnPath(orig.getPath()));
        return ret;
    }

    @Override
    public String getScheme() {
        return myUri.getScheme();
    }

    @Override
    public void access(Path path, FsAction mode) throws AccessControlException,
            FileNotFoundException, IOException {

    }



    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
            throws  IOException {
        return this.listLocatedStatus(f,null);
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
                                                                  final PathFilter filter)
            throws  IOException {

        FileStatus[] listing=this.listStatus(f);
        ArrayList<FileStatus> results = new ArrayList<>();
        for (int i = 0; i < listing.length; i++) {
            if (filter!=null)
            {
                if (filter.accept(listing[i].getPath())) {
                    results.add(listing[i]);
                }
            }else {
                results.add(listing[i]);
            }
        }

       return  new RemoteIterator<LocatedFileStatus>() {
            private final FileStatus[] stats = results.toArray(new FileStatus[results.size()]);
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i<stats.length;
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more entries in " + f);
                }
                FileStatus result = stats[i++];
                // for files, use getBlockLocations(FileStatus, int, int) to avoid
                // calling getFileStatus(Path) to load the FileStatus again
                BlockLocation[] locs = result.isFile() ?
                        getFileBlockLocations(result, 0, result.getLen()) :
                        null;
                return new LocatedFileStatus(result, locs);
            }
        };

    }


    public Path resolvePath(Path p) throws IOException {
        this.checkPath(p);
        try {
            return this.getFileStatus(p).getPath();
        }catch (IOException e)
        {
            LOG.error(e);
            throw e;
        }
    }


    public MfsFileSystem() {
    }

    public void initialize(URI name, Configuration conf) throws IOException {

        try {

            Configuration tmp=new Configuration(conf);
            tmp.set("fs.hdfs.impl.disable.cache","true");
            tmp.set("fs.jfs.impl.disable.cache","true");
            LOG.info("init new mfs filesystem");
            this.jfsScheme = "jfs";
            this.jfsAuthority = conf.get("juicefs.name");

            URI realUri=new URI(conf.get("fs.defaultFS","file:///"));
            this.hdfsScheme = realUri.getScheme();
            this.hdfsAuthority = realUri.getAuthority();
            this.hdfsUri = realUri;


            this.fs = FileSystem.get(realUri, tmp);
            assert this.fs != null;
            this.statistics = fs.statistics;


            this.jfs = FileSystem.get(new URI("jfs://"+jfsAuthority),tmp);

            this.myScheme = "mfs";
            this.myAuthority = "sz-cluster";
            this.myUri = new URI("mfs://sz-cluster/");
            super.initialize(realUri, conf);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public URI getUri() {
        return this.myUri;
    }


    public String getName() {
        return this.getUri().toString();
    }

    public Path makeQualified(Path path) {
        return this.swizzleReturnPath(super.makeQualified(this.swizzleHdfsPath(path)));
    }

    protected void checkPath(Path path) {
        super.checkPath(this.swizzleHdfsPath(path));
    }

    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        if (hdfsExists(swizzleHdfsPath(file.getPath()))) {
            return super.getFileBlockLocations(this.swizzleFileStatus(file, true,false), start, len);
        }
        return jfs.getFileBlockLocations(this.swizzleFileStatus(file, true,true), start, len);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        return fs.getAclStatus(this.swizzleHdfsPath(path));
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        fs.modifyAclEntries(this.swizzleHdfsPath(path), aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        fs.removeAclEntries(this.swizzleHdfsPath(path), aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        fs.removeDefaultAcl(this.swizzleHdfsPath(path));
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        fs.removeAcl(this.swizzleHdfsPath(path));
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        fs.setAcl(this.swizzleHdfsPath(path), aclSpec);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        if (hdfsExists(swizzleHdfsPath(f))) {
            return fs.getFileChecksum(this.swizzleHdfsPath(f), length);
        }
        return jfs.getFileChecksum(this.swizzleJfsPath(f), length);
    }

    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if (hdfsExists(swizzleHdfsPath(f))) {
            return super.open(this.swizzleHdfsPath(f), bufferSize);
        }
        return jfs.open(this.swizzleJfsPath(f), bufferSize);
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {

        if(this.hdfsExists(this.swizzleHdfsPath(p)))
        {
            return fs.getStatus(this.swizzleHdfsPath(p));
        }

        return jfs.getStatus(this.swizzleJfsPath(p));

    }

    public boolean supportsSymlinks() {
        return false;
    }


    @Override
    public boolean mkdirs(Path f) throws IOException {
        return fs.mkdirs(this.swizzleHdfsPath(f));
    }

    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return super.append(this.swizzleHdfsPath(f), bufferSize, progress);
    }

    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {

        if (!hdfsExists(this.swizzleHdfsPath(f.getParent())))
        {
            this.mkdirs(f.getParent());
        }
        return super.create(this.swizzleHdfsPath(f), permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    public boolean setReplication(Path src, short replication) throws IOException {
        return super.setReplication(this.swizzleHdfsPath(src), replication);
    }

    public boolean rename(Path src, Path dst) throws IOException {

        if(!this.hdfsExists(this.swizzleHdfsPath(src)))
        {
            return true;
        }

        Path path=this.swizzleHdfsPath(dst);
        fs.mkdirs(path.getParent());
        return super.rename(this.swizzleHdfsPath(src), path);

    }

    @Override
    protected void rename(Path src, Path dst, Options.Rename... options)
            throws IOException {
        Path path=this.swizzleHdfsPath(dst);
        fs.mkdirs(path.getParent());
        fs.rename(this.swizzleHdfsPath(src), path, options);
    }

    public boolean delete(Path f, boolean recursive) throws IOException {
        if(!this.hdfsExists(this.swizzleHdfsPath(f)))
        {
            return true;
        }

        if(this.jfsExists(this.swizzleJfsPath(f)))
        {
            jfs.delete(this.swizzleJfsPath(f));
        }

        return  super.delete(this.swizzleHdfsPath(f), recursive);

    }

    public boolean deleteOnExit(Path f) throws IOException {


        if(!this.hdfsExists(this.swizzleHdfsPath(f)))
        {
            return true;
        }

        return super.deleteOnExit(this.swizzleHdfsPath(f));
    }

    public FileStatus[] listStatus(Path f) throws IOException {

        try {
            FileStatus[] orig = new FileStatus[0];
            if (hdfsExists(this.swizzleHdfsPath(f))) {
                orig = fs.listStatus(this.swizzleHdfsPath(f));
            }
            if(orig.length<=0)
            {
                if (jfsExists(this.swizzleJfsPath(f))) {
                    orig = jfs.listStatus(this.swizzleJfsPath(f));
                }
            }else {
                FileStatus[] jorig = new FileStatus[0];
                Set<String> set=new HashSet<>();
                for (int j = 0; j < orig.length; ++j) {
                    set.add(orig[j].getPath().getName());
                }

                if (jfsExists(this.swizzleJfsPath(f))) {
                    jorig = jfs.listStatus(this.swizzleJfsPath(f));
                }

                List<FileStatus> list=new ArrayList<>();
                for (int i = 0; i < jorig.length; ++i) {
                    if(jorig[i].isDirectory())
                    {
                        if(!set.contains(jorig[i].getPath().getName()))
                        {
                            list.add(jorig[i]);
                        }
                    }
                }

                FileStatus[] ret = new FileStatus[orig.length+list.size()];

                int index=0;
                for (int i = 0; i < orig.length; ++i) {
                    ret[i] = this.swizzleFileStatus(orig[i], false,false);
                    index++;
                }

                for (int i = 0; i < list.size(); ++i) {
                    ret[index] = this.swizzleFileStatus(list.get(i), false,false);
                    index++;
                }

                return ret;

            }


            FileStatus[] ret = new FileStatus[orig.length];

            for (int i = 0; i < orig.length; ++i) {
                ret[i] = this.swizzleFileStatus(orig[i], false,false);
            }

            return ret;
        }catch (IOException e)
        {
            LOG.error(e);
            throw e;
        }


    }

    private boolean hdfsExists(Path p) throws IOException {
        try {
            return fs.getFileStatus(p) != null;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw e;
        }
    }

    private boolean jfsExists(Path p) throws IOException {
        try {
            return jfs.getFileStatus(p) != null;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw e;
        }
    }

    public Path getHomeDirectory() {
        return this.swizzleReturnPath(super.getHomeDirectory());
    }

    public void setWorkingDirectory(Path newDir) {
        super.setWorkingDirectory(this.swizzleHdfsPath(newDir));
    }

    public Path getWorkingDirectory() {
        return this.swizzleReturnPath(super.getWorkingDirectory());
    }

    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return super.mkdirs(this.swizzleHdfsPath(f), permission);
    }

    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        throw new IOException("mfs not support this action");
    }

    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
        throw new IOException("mfs not support this action");
    }

    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        throw new IOException("mfs not support this action");
    }

    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        if (!jfsExists(this.swizzleJfsPath(src))) {
            throw new IOException("file not exists,please wait to sync");
        }
        jfs.copyToLocalFile(delSrc, this.swizzleJfsPath(src), dst);
    }

    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        if (!jfsExists(this.swizzleJfsPath(fsOutputFile))) {
            throw new IOException("file not exists,please wait to sync");
        }
        return jfs.startLocalOutput(this.swizzleJfsPath(fsOutputFile), tmpLocalFile);

    }

    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        jfs.completeLocalOutput(this.swizzleJfsPath(fsOutputFile), tmpLocalFile);

    }

    public ContentSummary getContentSummary(Path f) throws IOException {
        if(this.hdfsExists(this.swizzleHdfsPath(f)))
        {
            return fs.getContentSummary(this.swizzleHdfsPath(f));
        }
        return jfs.getContentSummary(this.swizzleJfsPath(f));
    }

    public FileStatus getFileStatus(Path f) throws IOException {
        try{
            FileStatus fs = this.fs.getFileStatus(this.swizzleHdfsPath(f));
            return this.swizzleFileStatus(fs, false,false);
        }catch (FileNotFoundException ex)
        {
            return this.swizzleFileStatus(jfs.getFileStatus(this.swizzleJfsPath(f)), false,false);
        }
    }

    @Override
    public void close() throws IOException {
        this.jfs.close();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value)
            throws IOException {
        fs.setXAttr(this.swizzleHdfsPath(path), name, value);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
                         EnumSet<XAttrSetFlag> flag) throws IOException {
        fs.setXAttr(this.swizzleHdfsPath(path), name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        return fs.getXAttr(this.swizzleHdfsPath(path), name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        return fs.getXAttrs(this.swizzleHdfsPath(path));
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException {
        return fs.getXAttrs(this.swizzleHdfsPath(path), names);
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        return fs.listXAttrs(this.swizzleHdfsPath(path));
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        fs.removeXAttr(this.swizzleHdfsPath(path), name);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path f)
            throws IOException {

        FileStatus[] listing=this.listStatus(f);
        ArrayList<FileStatus> results = new ArrayList<>();
        for (int i = 0; i < listing.length; i++) {
            results.add(listing[i]);
        }
        return  new RemoteIterator<FileStatus>() {
            private final FileStatus[] stats = results.toArray(new FileStatus[results.size()]);
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i<stats.length;
            }

            @Override
            public FileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more entries in " + f);
                }
                FileStatus result = stats[i++];
                // for files, use getBlockLocations(FileStatus, int, int) to avoid
                // calling getFileStatus(Path) to load the FileStatus again

                return result;
            }
        };

    }

    public FileChecksum getFileChecksum(Path f) throws IOException {
        return this.getFileChecksum(f,Long.MAX_VALUE);
    }

    public void setOwner(Path p, String username, String groupname) throws IOException {
        super.setOwner(this.swizzleHdfsPath(p), username, groupname);
    }

    public void setTimes(Path p, long mtime, long atime) throws IOException {
        super.setTimes(this.swizzleHdfsPath(p), mtime, atime);
    }

    public void setPermission(Path p, FsPermission permission) throws IOException {
        super.setPermission(this.swizzleHdfsPath(p), permission);
    }

}