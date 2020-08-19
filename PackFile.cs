using System;
using System.Text;
using System.IO;
using System.IO.Compression;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Linq;

namespace PackFile
{
    public class PackFile
    {
        public PackFile()
        {
            packfileName = string.Empty;
            connectionStringBuilder = new SqliteConnectionStringBuilder();
            CreationMode = CreateMode.ErrorOnExist;
            memoryConnection = null;
        }

        ~PackFile()
        {
            if (memoryConnection != null)
                {
                    memoryConnection.Close();
                    memoryConnection = null;
                }
        }

        // Defines how to handle packfile creation if file already exists
        public enum CreateMode {AppendExist, OverWriteExist, ErrorOnExist}
        public string[] IncludeMasks {get; set;}
        public string[] ExcludeMasks {get; set;}

        public CreateMode CreationMode {get; set;}
        
        public void Intialize()
        {
            if (string.IsNullOrEmpty(packfileName))
            {
                CreateTables();
                return;
            }

            if (CreateBareFile(packfileName))
            {
                CreateTables();
            }
        }

        // Set PackFileName to string.empty to create in-memory db
        public string PackFileName
        {
            get
            {
                return packfileName;
            }
            set
            {
                packfileName = value;
                connectionStringBuilder.Clear();
                if (string.IsNullOrEmpty(packfileName))
                {
                    connectionStringBuilder.ConnectionString = "Data Source=InMemoryPack;Mode=Memory;Cache=Shared";
                    memoryConnection = new SqliteConnection(connectionStringBuilder.ToString());
                    memoryConnection.Open();
                }
                else
                {
                    connectionStringBuilder.DataSource = packfileName;
                    if (memoryConnection != null)
                    {
                        memoryConnection.Close();
                        memoryConnection = null;
                    }
                }
            }
        }

        public enum Compression {None=0, Deflate=1, GZip=2, Brotli=3}
        public void Add(string path, PackFile.Compression compression=PackFile.Compression.None)
        {
            //Directory 0 is root
            if ((File.GetAttributes(path) & FileAttributes.Directory) == FileAttributes.Directory)
            {
                AddDir(path, 0, compression);
            }
            else
            {
                AddFile(path, 0, compression);
            } 
        }

        public void Add(IEnumerable<string> paths, PackFile.Compression compression=PackFile.Compression.None)
        {
            foreach (string path in paths)
            {
                Add(path.Trim(), compression);
            }
        }

        private void AddFile(string fileName, long dirEntry, PackFile.Compression compression)
        {
            FileInfo fi = new FileInfo(fileName);
            byte[] data = File.ReadAllBytes(fileName);
            using (SqliteConnection con = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                con.Open();
                SqliteCommand insertCmd = con.CreateCommand();
                insertCmd.CommandText = @"INSERT INTO files(filename, dir, attributes, creationTime, writeTime, accessTime, encoding, data) 
                    VALUES (@filename, @dir, @attributes, strftime('%s', @creationTime), strftime('%s', @writeTime), strftime('%s', @accessTime), @encoding, @data)";
                insertCmd.Parameters.AddWithValue("@filename", Path.GetFileName(fi.FullName));
                insertCmd.Parameters.AddWithValue("@dir", dirEntry); 
                insertCmd.Parameters.AddWithValue("@attributes", (int) fi.Attributes);
                insertCmd.Parameters.AddWithValue("@creationTime", fi.CreationTimeUtc);
                insertCmd.Parameters.AddWithValue("@writeTime", fi.LastWriteTimeUtc);
                insertCmd.Parameters.AddWithValue("@accessTime", fi.LastAccessTimeUtc);
                insertCmd.Parameters.AddWithValue("@encoding", (byte) compression);
                insertCmd.Parameters.AddWithValue("@data", Compress(data, compression));
                
                insertCmd.ExecuteNonQuery();
                con.Close();
            }
        }

        private long AddDir(string dirName, long parentDir, PackFile.Compression compression)
        {
            FileInfo di = new FileInfo(dirName);
            long thisDirId = -1;
            using (SqliteConnection con = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                con.Open();
                SqliteCommand insertCmd = con.CreateCommand();
                insertCmd.CommandText = @"INSERT INTO dirs (dirname, attributes, creationTime, writeTime, accessTime, parent)
                 VALUES (@dirname, @attributes,  strftime('%s', @creationTime), strftime('%s', @writeTime), strftime('%s', @accessTime), @parent)";
                insertCmd.Parameters.AddWithValue("@dirname", Path.GetFileName(di.FullName));
                insertCmd.Parameters.AddWithValue("@attributes", (int) di.Attributes);
                insertCmd.Parameters.AddWithValue("@creationTime", di.CreationTimeUtc);
                insertCmd.Parameters.AddWithValue("@writeTime", di.LastWriteTimeUtc);
                insertCmd.Parameters.AddWithValue("@accessTime", di.LastAccessTimeUtc);
                insertCmd.Parameters.AddWithValue("@parent", parentDir);

                insertCmd.ExecuteNonQuery();

                SqliteCommand getThisRowId = con.CreateCommand();
                getThisRowId.CommandText = "SELECT last_insert_rowid()";

                thisDirId = (long) getThisRowId.ExecuteScalar();

                con.Close();
            }
            //HashSets ensure unique entries.
            HashSet<string> filesToInclude = new HashSet<string>();
            // if no include masks are specified, include all.
            if (IncludeMasks.Length == 0)
            {
                IncludeMasks = new string[] {"*"};
            }
            foreach (string mask in IncludeMasks)
            {
                string[] tempList = Directory.GetFiles(di.FullName, mask, SearchOption.TopDirectoryOnly);
                foreach (string entry in tempList)
                {
                    filesToInclude.Add(entry);
                }
            }
            //Now do more or less the same, for exclusions
            HashSet<string> filesToExclude = new HashSet<string>();
            foreach (string mask in ExcludeMasks)
            {
                string[] tempList = Directory.GetFiles(di.FullName, mask, SearchOption.TopDirectoryOnly);
                foreach (string entry in tempList)
                {
                    filesToExclude.Add(entry);
                }
            }
            
            //Now subtract exclude from include and you have your file list.
            string[] files = filesToInclude.ToArray().Except(filesToExclude.ToArray()).ToArray();
            
            foreach (string file in files)
            {
                AddFile(file, thisDirId, compression);
            }
            //recusively call for all subdirs
            string[] subdirs = Directory.GetDirectories(di.FullName, "*", SearchOption.TopDirectoryOnly);
            foreach (string dir in subdirs)
            {
                AddDir(dir, thisDirId, compression);
            }

            return thisDirId;
        }


        public void BackupTo(string Destination)
        {
            CreateBareFile(Destination);
            SqliteConnectionStringBuilder destSB = new SqliteConnectionStringBuilder();
            destSB.DataSource = Destination;
            Backup(connectionStringBuilder.ConnectionString, destSB.ConnectionString);
        }

        public void BackupFrom(string fileSource)
        {
            SqliteConnectionStringBuilder source = new SqliteConnectionStringBuilder();
            source.DataSource = fileSource;
            Backup(source.ToString(), connectionStringBuilder.ConnectionString);
        }

        private void Backup(string src, string dest)
        {
            using (SqliteConnection destination = new SqliteConnection(dest))
            {
                destination.Open();
                using (SqliteConnection source = new SqliteConnection(src))
                {
                    source.Open();
                    source.BackupDatabase(destination);
                    source.Close();
                }
                destination.Close();
            }
        }

        //Returns true if file created. Handles CreateMode logic.
        private bool CreateBareFile(string fileName)
        {
            bool createdNew = false;
            if (File.Exists(fileName))
            {
                if (CreationMode == CreateMode.ErrorOnExist)
                {
                    throw new InvalidOperationException("Packfile " + fileName + " exists and append/overwrite not specified."); 
                }
                if (CreationMode == CreateMode.OverWriteExist)
                {
                    File.Delete(fileName);
                    File.Create(fileName).Close();
                    createdNew = true;
                }
            }
            else
            {
                File.Create(fileName).Close();
                createdNew = true;
            }
            return createdNew;
        }

        private byte[] Compress(byte[] input, PackFile.Compression compression)
        {
            if (compression == PackFile.Compression.None)
            {
                return input;
            }
            MemoryStream output = new MemoryStream();
            Stream compressionStream;
            switch (compression)
            {                 
                case PackFile.Compression.Deflate:
                    compressionStream = new DeflateStream(output, CompressionLevel.Optimal);
                    break;
                case PackFile.Compression.GZip:
                    compressionStream = new GZipStream(output, CompressionLevel.Optimal);
                    break;
                case PackFile.Compression.Brotli:
                    compressionStream = new BrotliStream(output, CompressionLevel.Optimal);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("compression", "Unsupported compression type");
            }

            compressionStream.Write(input, 0, input.Length);
            compressionStream.Dispose();
            return output.ToArray();
        }

        private byte[] Decompress(byte[] compressedData, PackFile.Compression compression)
        {
            if (compression == PackFile.Compression.None)
            {
                return compressedData;
            }
            MemoryStream input = new MemoryStream(compressedData);
            MemoryStream output = new MemoryStream();
            Stream decompressStream;
            switch (compression)
            {                 
                case PackFile.Compression.Deflate:
                    decompressStream = new DeflateStream(input, CompressionMode.Decompress);
                    break;
                case PackFile.Compression.GZip:
                    decompressStream = new GZipStream(input, CompressionMode.Decompress);
                    break;
                case PackFile.Compression.Brotli:
                    decompressStream = new BrotliStream(input, CompressionMode.Decompress);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("compression", "Unsupported compression type");
            }

            decompressStream.CopyTo(output);
            decompressStream.Dispose();
            return output.ToArray();
        }

        public void ExtractAll(string destination)
        {
            if (!Directory.Exists(destination))
            {
                Directory.CreateDirectory(destination);
            }
            long maxDir = -1;
            using (SqliteConnection con = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                con.Open();
                // Get directory depth               
                maxDir = GetMaxDirectoryDepth(con);
                
                // Get all the files in this directory
                SqliteCommand getFiles = con.CreateCommand();
                getFiles.CommandText = "SELECT filename, encoding, data, creationTime, writeTime, accessTime, attributes FROM files WHERE dir=@dir";
                getFiles.Parameters.Add("@dir", SqliteType.Integer);            

                // Get all subdirectories in this directory
                SqliteCommand getSubdirs = con.CreateCommand();
                getSubdirs.CommandText = "SELECT dirname, creationTime, writeTime, accessTime, attributes FROM dirs WHERE parent=@dir";
                getSubdirs.Parameters.Add("@dir", SqliteType.Integer);
                
                for (long dir=0; dir<=maxDir; dir++)
                {
                    // Get the full destination path
                    string fullPath = GetFullPath(dir, con, destination);
                   
                    // Extract all files in this directory
                    getFiles.Parameters["@dir"].Value = dir;
                    using (SqliteDataReader reader = getFiles.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string outfile = fullPath + reader["filename"];
                            FileStream fs = File.Create(outfile);
                            PackFile.Compression encoding = (PackFile.Compression) Convert.ToInt32(reader["encoding"]);
                            if (reader["data"] != null && !Convert.IsDBNull(reader["data"]))
                            {
                                //Read data into buffer & write to file
                                byte[] data = Decompress((byte[]) reader["data"], encoding);
                                fs.Write(data, 0, data.Length);
                            }
                            fs.Close();

                            FileInfo fixupFI = new FileInfo(outfile);
                            //Now fixup the metadata
                            fixupFI.CreationTimeUtc = DateTimeOffset.FromUnixTimeSeconds((long) reader["creationTime"]).DateTime;
                            fixupFI.LastWriteTimeUtc = DateTimeOffset.FromUnixTimeSeconds((long) reader["writeTime"]).DateTime;
                            fixupFI.LastAccessTimeUtc  = DateTimeOffset.FromUnixTimeSeconds((long) reader["accessTime"]).DateTime;
                            fixupFI.Attributes = (FileAttributes) Convert.ToInt32(reader["attributes"]);
                        }
                    }

                    // Extract all subdirectories
                    getSubdirs.Parameters["@dir"].Value = dir;
                    using (SqliteDataReader reader = getSubdirs.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            DirectoryInfo di = Directory.CreateDirectory(fullPath + reader["dirname"]);
                            di.CreationTimeUtc = DateTimeOffset.FromUnixTimeSeconds((long) reader["creationTime"]).DateTime;
                            di.LastWriteTimeUtc = DateTimeOffset.FromUnixTimeSeconds((long) reader["writeTime"]).DateTime;
                            di.LastAccessTimeUtc  = DateTimeOffset.FromUnixTimeSeconds((long) reader["accessTime"]).DateTime;
                            di.Attributes = (FileAttributes) Convert.ToInt32(reader["attributes"]);
                        }
                    }
                }

                con.Close();
            }
        }

        public string[] List()
        {
            List<string> entries = new List<string>();
            using (SqliteConnection con = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                con.Open();
                SqliteCommand listCmd = con.CreateCommand();
                listCmd.CommandText = "SELECT filename FROM files WHERE dir=@dir";
                listCmd.Parameters.Add("@dir", SqliteType.Integer);
                long dirDepth = GetMaxDirectoryDepth(con);

                for (long dir=0; dir<=dirDepth; dir++)
                {
                    string fullPath = GetFullPath(dir, con, string.Empty);
                    listCmd.Parameters["@dir"].Value = dir;
                    using (SqliteDataReader reader = listCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            entries.Add(fullPath + reader["filename"].ToString());
                        }
                    }
                }

                con.Close();
            }
            return entries.ToArray();
        }

        private string GetFullPath(long dir, SqliteConnection con, string rootname = "./")
        {
            SqliteCommand getPath = con.CreateCommand();
            getPath.CommandText = "SELECT dirname, parent FROM dirs WHERE id=@dir";
            getPath.Parameters.Add("@dir", SqliteType.Integer);
            string fullPath = string.Empty;
            long tempdir = dir;
            do 
            {
                getPath.Parameters["@dir"].Value = tempdir;
                using (SqliteDataReader reader = getPath.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        fullPath = reader["dirname"] + "/" + fullPath;
                        tempdir = (long) reader["parent"];
                    }
                }

            } while (tempdir >= 0);

            return rootname + fullPath;
        }
        private long GetMaxDirectoryDepth(SqliteConnection con)
        {
            SqliteCommand getMaxDir = con.CreateCommand();
            getMaxDir.CommandText = "SELECT MAX(id) FROM dirs";
            return (long) getMaxDir.ExecuteScalar();
        }
        private string packfileName;

        private SqliteConnectionStringBuilder connectionStringBuilder;

        // Holds open a connection to in-memory db, to prevent reclaiming.
        private SqliteConnection memoryConnection;

        private void CreateTables()
        {
            using (SqliteConnection con = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                con.Open();
                using (SqliteTransaction transaction = con.BeginTransaction())
                {
                    // Pragma to make sure sqlite supports foreign keys
                    SqliteCommand fkPragmaCmd = con.CreateCommand();
                    fkPragmaCmd.CommandText = "PRAGMA foreign_keys = ON;";
                    fkPragmaCmd.ExecuteNonQuery();

                    // By keeping directories in a seperate table, it makes it easier to make sure that a given subdirectory
                    // exists when we are extracting files.
                    // Create the dirs table
                    StringBuilder dirsTableCmdText = new StringBuilder("CREATE TABLE IF NOT EXISTS dirs(");
                    dirsTableCmdText.Append("id INTEGER PRIMARY KEY AUTOINCREMENT");
                    dirsTableCmdText.Append(", dirname TEXT NOT NULL");
                    dirsTableCmdText.Append(", attributes INT4");
                    dirsTableCmdText.Append(", creationTime INT8");
                    dirsTableCmdText.Append(", writeTime INT8");
                    dirsTableCmdText.Append(", accessTime INT8");
                    dirsTableCmdText.Append(", parent INT8");
                    dirsTableCmdText.Append(");");

                    SqliteCommand dirsTableCmd = con.CreateCommand();
                    dirsTableCmd.CommandText = dirsTableCmdText.ToString();
                    dirsTableCmd.ExecuteNonQuery();

                    // Insert the root of the packfile
                    SqliteCommand rootDirCmd = con.CreateCommand();
                    rootDirCmd.CommandText = "INSERT INTO DIRS(id, dirname, parent) VALUES (0, '.', -1);";
                    rootDirCmd.ExecuteNonQuery();

                    // Create the files table
                    StringBuilder fileTableCmdText = new StringBuilder("CREATE TABLE IF NOT EXISTS files(");
                    fileTableCmdText.Append("filename TEXT NOT NULL");
                    fileTableCmdText.Append(", dir INTEGER NOT NULL");
                    fileTableCmdText.Append(", attributes INT4");
                    fileTableCmdText.Append(", creationTime INT8");
                    fileTableCmdText.Append(", writeTime INT8");
                    fileTableCmdText.Append(", accessTime INT8");
                    fileTableCmdText.Append(", encoding INT1");
                    fileTableCmdText.Append(", data BLOB");
                    fileTableCmdText.Append(", FOREIGN KEY (dir) REFERENCES dirs (id)");
                    fileTableCmdText.Append(");");

                    SqliteCommand fileTableCmd = con.CreateCommand();
                    fileTableCmd.CommandText = fileTableCmdText.ToString();
                    fileTableCmd.ExecuteNonQuery();

                    transaction.Commit();
                }
                con.Close();
            }
        }
    }
}